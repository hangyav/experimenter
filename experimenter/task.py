from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import re
import os
import shutil
import dask

from experimenter.executor import CliExecutor, DummyExecutor


logger = logging.getLogger(__name__)

class TaskDefinition:

    def __init__(self, actions=None, name=None, params=None, patterns=None, dependencies=None, executor=CliExecutor,
                 outputs=None):
        if (actions is None or len(actions) == 0) and len(dependencies) == 0:
            raise ValueError('At least one action or one dependency has to be defined!')

        if patterns is None and name is None:
            raise ValueError('Name or pattern has to be defined!')

        self.actions = actions
        self.name = name
        if params is None:
            params = dict()
        self.params = params
        self.patterns = patterns
        self.dependencies = dependencies
        self.executor = executor
        self.outputs = outputs

    def __str__(self):
        return '{}: {}'.format(self.name, self.params)

    def create_instance(self, pool, pattern=None):
        params = self.params
        if params is None:
            params = dict()

        if pattern is not None and pattern[1] is not None:
            match = re.search(pattern[1], pattern[0])
            params['MATCH'] = pattern[0]
            for name, value in match.groupdict().items():
                params[name] = value

        ##########################################################
        dependencies = list()
        latest_parent_modification = -1.0
        if self.dependencies is not None:
            for dep_idx, dep in enumerate(self.dependencies):
                dep = TaskDefinition._get_param(dep, params)

                key = 'DEP{}'.format(dep_idx)
                if key in params:
                    raise ValueError('Invalid parameter name: {}'.format(key))
                params[key] = dep

                dep_task = None
                pattern = None
                tmp = pool.lookup_task_by_pattern(dep)
                if tmp is not None:
                    dep_task = tmp[0]
                    pattern = tmp[1]
                if dep_task is None and dep in pool.named_tasks: #look for the exact task name
                    dep_task = pool.named_tasks[dep]

                if dep_task is None:
                    if not os.path.exists(dep):
                        raise ValueError('File dependency does not exists: {}'.format(dep))
                    continue

                ##########################################################
                dep_instance = dep_task.create_instance(pool, pattern=(dep, pattern))
                if dep_instance[0] is not None:
                    dependencies.append(dep_instance[0])
                if dep_instance[1] is not None:
                    latest_parent_modification = max(latest_parent_modification, dep_instance[1])

        ##########################################################

        outputs = list()
        if self.outputs is not None:
            outputs = [TaskDefinition._get_param(o, params) for o in self.outputs]
            for oidx, o in enumerate(outputs):
                key = 'OUT{}'.format(oidx)
                if key in params:
                    raise ValueError('Invalid parameter name: {}'.format(key))
                params[key] = o

        #########################################################

        tasks = None
        if self.actions is not None:
            tasks = self.executor([TaskDefinition._get_param(task, params) for task in self.actions])

        ##########################################################

        earliest_modification = self._earliest_output_modification_time(outputs)
        if earliest_modification is None:
            earliest_modification = -1.0

        if len(outputs) > 0 and not self._is_output_exists(outputs):
            pass
        elif len(dependencies) > 0:
            pass
        elif len(outputs) > 0 and latest_parent_modification > earliest_modification:
            pass
        else:
            logger.info('All dependencies and outputs are satisfied for task: {}'.format(self))
            return (None, self._latest_output_modification_time(outputs))

        return (TaskInstance(tasks, dependencies, self, pool, outputs), self._latest_output_modification_time(outputs))

    @staticmethod
    def _is_output_exists(outputs):
        if outputs is None or len(outputs) == 0:
            return True

        for o in outputs:
            if not os.path.exists(o):
                return False

        return True

    @staticmethod
    def _getmtime(x):
        if os.path.isdir(x):
            return None
        return os.path.getmtime(x)

    @staticmethod
    def _output_modification_time(outputs, func):
        if outputs is None or len(outputs) == 0:
            return None

        o = outputs[0]
        res = None
        if os.path.exists(o):
            res = TaskDefinition._getmtime(o)
        for o in outputs[1:]:
            if os.path.exists(o):
                res = func(res, TaskDefinition._getmtime(o))

        return res

    @staticmethod
    def _get_param(x, params):
        if type(x) == tuple:
            return x[0](*[v.format(**params) for v in x[1:]])

        return x.format(**params)

    @staticmethod
    def _earliest_output_modification_time(outputs):
        return TaskDefinition._output_modification_time(outputs, min)

    @staticmethod
    def _latest_output_modification_time(outputs):
        return TaskDefinition._output_modification_time(outputs, max)


class TaskInstance:

    def __init__(self, task, dependencies, definition, pool, outputs):
        assert (task is not None and len(task.commands) > 0) or len(dependencies) > 0, 'Error no task nor dependency for task instance!'

        self.task = task
        self.dependecies = dependencies
        self.definition = definition
        self.pool = pool
        self.outputs = outputs

    def execute(self):

        dep_delays = [dep.execute() for dep in self.dependecies]

        if self.task is not None:
            d = dask.delayed(self.task.execute)(self, dependencies=dep_delays)
            if self.task in self.pool.task_instances:
                d = self.pool.task_instances[self.task]
            else:
                self.pool.task_instances[self.task] = d
            return d
        else:
            return dask.delayed(DummyExecutor().execute)(dependencies=dep_delays)

    def started(self):
        self.pool.active_tasks.add(self)

    def stoped(self):
        self.pool.active_tasks.remove(self)

    def recover(self):
        if self.outputs is not None:
            for o in self.outputs:
                if os.path.exists(o):
                    logger.warning('Removing non-finished output: {}'.format(o))
                    if os.path.isdir(o):
                        shutil.rmtree(o)
                    else:
                        os.remove(o)


class TaskPool:

    def __init__(self, tasks, main=None, num_workers=1):
        self.tasks = tasks
        self.named_tasks = {task.name: task for task in tasks if task.name is not None}
        self.main = main
        self.num_workers = num_workers
        self.task_instances = dict()

        self.active_tasks = None

        self._patterns = list()
        for task in tasks:
            if task.patterns is None:
                continue

            for dep in task.patterns:
                self._patterns.append((re.compile('^{}$'.format(dep)), task))

    @staticmethod
    def init_from_py(file, variables=None):
        vars = dict()
        vars['load'] = load
        vars['var'] = var
        vars['TaskDefinition'] = TaskDefinition

        if variables is not None:
            for cmdvar in variables:
                cv = cmdvar.split('=')
                if len(cv) != 2:
                    raise ValueError('Invalid command line variable: {}'.format(cmdvar))
                vars[cv[0]] = cv[1]

        load(file, vars)
        tasks = [var for name, var in vars.items() if isinstance(var, TaskDefinition)]
        main_task = None
        if 'main' in vars:
            main_task = vars['main']
        return TaskPool(tasks, main=main_task)

    def lookup_task_by_pattern(self, name):
        found = [(task, pattern) for pattern, task in self._patterns if pattern.match(name) is not None]

        if len(found) > 1:
            raise ValueError('{} matched multiple times: {}'.format(name, ', '.join([task.__str__() for task, _ in found])))

        if len(found) == 0:
            return None
        return found[0]

    def _get_actual_tasks(self, tasks):
        res = list()

        if not isinstance(tasks, list):
            tasks = [tasks]

        for item in tasks:
            if isinstance(item, TaskDefinition):
                res.append(item.create_instance(self))
            elif isinstance(item, str):
                if item in self.named_tasks:
                    res.append(self.named_tasks[item].create_instance(self))
                else:
                    task = self.lookup_task_by_pattern(item)
                    if task is None:
                        raise ValueError('No such task: {}'.format(item))
                    res.append(task[0].create_instance(self, pattern=(item, task[1])))
            else:
                raise ValueError('Invalid task: {}'.format(item))

        return res

    def execute(self, task=None):
        if task is None:
            task = self.main

            if task is None:
                raise ValueError('No main task is set!')

        delays = list()
        for task in self._get_actual_tasks(task):
            if task[0] is not None:
                delays.append(task[0].execute())

        try:
            print('\033[1m\033[91mNumber of tasks to run: {}\033[0m'.format(len(self.task_instances)))
            self.active_tasks = set()
            dask.compute(*delays, num_workers=self.num_workers)
        except BaseException as e:
            self.handle_error()
            self.active_tasks = None
            raise e

    def handle_error(self):
        for task in self.active_tasks:
            task.recover()


# FIXME the useage of these methods is not at all elegant
def load(file, vars):
    import sys
    if sys.version_info[0] == 2:
        importer = execFile
    else:
        importer = exec

    with open(file, 'r') as fin:
        importer(fin.read(), vars)

def var(name, value, vars):
    if name in vars:
        return vars[name]
    return value
