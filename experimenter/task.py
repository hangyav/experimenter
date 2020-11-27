from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import re
import os
import shutil
from functools import partial

from experimenter.cluster.rpyc_cluster import GPU

from experimenter import executor


logger = logging.getLogger(__name__)

class TaskDefinition:

    def __init__(self, actions=None, name=None, params=None, patterns=None,
                 dependencies=None, executor=executor.CliExecutor, outputs=None,
                 resources=None):
        if (actions is None or len(actions) == 0) and len(dependencies) == 0:
            raise ValueError('At least one action or one dependency has to be defined!')

        self.actions = actions
        self.name = name
        if params is None:
            params = dict()
        self.params = params
        self.patterns = patterns
        self.dependencies = dependencies
        self.executor = executor
        self.outputs = outputs
        self.resources = resources if resources is not None else {}

    def __str__(self):
        return '{}: {} {}'.format(self.name, self.params, self.patterns)

    def create_instance(self, pool, force_run, pattern=None):
        if self.params is None:
            params = dict()
        else:
            params = self.params.copy()

        if pattern is not None and pattern[1] is not None:
            match = re.search(pattern[1], pattern[0])
            params['MATCH'] = pattern[0]
            for name, value in match.groupdict().items():
                if value is not None:
                    params[name] = value

        ##########################################################
        dependencies = list()
        latest_parent_modification = -1.0
        if self.dependencies is not None:
            for dep_idx, dep in enumerate(self.dependencies):
                dep = TaskDefinition._get_param(dep, params)

                if dep is None:
                    dep = 'None'

                key = 'DEP{}'.format(dep_idx)
                if key in params:
                    raise ValueError('Invalid parameter name: {}'.format(key))
                params[key] = dep

                if dep.lower() == 'none':
                    continue

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
                        raise ValueError('File dependency does not exists (Task: {}): {}'.format(self.name, dep))
                    continue

                ##########################################################
                dep_instance = dep_task.create_instance(pool, force_run=force_run, pattern=(dep, pattern))
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

        earliest_modification = self._earliest_output_modification_time(outputs, follow_links=False)
        if earliest_modification is None:
            earliest_modification = -1.0

        if outputs is None or len(outputs) == 0:
            pass
        elif not self._is_output_exists(outputs):
            pass
        elif len(dependencies) > 0:
            pass
        elif len(outputs) > 0 and latest_parent_modification > earliest_modification:
            pass
        elif force_run:
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
    def _getmtime(x, follow_links=True):
        if os.path.isdir(x):
            return None
        if follow_links:
            return os.path.getmtime(x)
        return os.lstat(x).st_mtime

    @staticmethod
    def _output_modification_time(outputs, func, follow_links=True):
        if outputs is None or len(outputs) == 0:
            return None

        o = outputs[0]
        res = None
        if os.path.exists(o):
            res = TaskDefinition._getmtime(o, follow_links=follow_links)
            for o in outputs[1:]:
                if os.path.exists(o):
                    res = func(res, TaskDefinition._getmtime(o, follow_links=follow_links))

        return res

    @staticmethod
    def _get_param(x, params):
        if x is None:
            return None
        elif type(x) == tuple:
            return TaskDefinition._get_param(x[0](*[TaskDefinition._get_param(v, params) for v in x[1:]]), params)
        elif type(x) == list:
            return [TaskDefinition._get_param(v, params) for v in x]
        elif type(x) == str:
            return x.format(**params)
        else:
            raise ValueError('Type {} not a supported parameter!'.format(type(x)))

    @staticmethod
    def _earliest_output_modification_time(outputs, follow_links=True):
        return TaskDefinition._output_modification_time(outputs, min, follow_links=follow_links)

    @staticmethod
    def _latest_output_modification_time(outputs, follow_links=True):
        return TaskDefinition._output_modification_time(outputs, max, follow_links=follow_links)


class ExecNode():

    def __init__(self, function, resources, dependencies=None):
        self.function = function
        self.resources = resources
        self.dependencies = dependencies


class TaskInstance:

    def __init__(self, task, dependencies, definition, pool, outputs):
        assert (task is not None and len(task.commands) > 0) or len(dependencies) > 0, 'Error no task nor dependency for task instance!'

        self.task = task
        self.dependecies = dependencies
        self.definition = definition
        self.pool = pool
        self.outputs = outputs

    def execute(self):

        dep_nodes = [dep.execute() for dep in self.dependecies]

        if self.task is not None:
            d = ExecNode(partial(self.task.execute, self), self.definition.resources,
                         dep_nodes)
            if self.task in self.pool.task_instances:
                d, _ = self.pool.task_instances[self.task]
            else:
                self.pool.task_instances[self.task] = (d, self)
            return d
        else:
            return ExecNode(partial(executor.DummyExecutor().execute), self.definition.resources,
                            dep_nodes)

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

    def __init__(self, tasks, scheduler, main=None):
        self.tasks = tasks
        self.named_tasks = {task.name: task for task in tasks if task.name is not None}
        self.main = main
        self.task_instances = dict()
        self.scheduler = scheduler

        self._patterns = list()
        for task in tasks:
            if task.patterns is None:
                continue

            for dep in task.patterns:
                self._patterns.append((re.compile('^{}$'.format(dep)), task))

    @staticmethod
    def init_from_py(file, scheduler, variables=None):
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
        tasks = list()
        for name, inst in vars.items():
            if isinstance(inst, TaskDefinition):
                if inst.name is None:
                    inst.name = name
                tasks.append(inst)
        #  tasks = [var for name, var in vars.items() if isinstance(var, TaskDefinition)]
        main_task = None
        if 'main' in vars:
            main_task = vars['main']
        return TaskPool(tasks, scheduler, main=main_task)

    def lookup_task_by_pattern(self, name):
        found = [(task, pattern) for pattern, task in self._patterns if pattern.match(name) is not None]

        if len(found) > 1:
            raise ValueError('{} matched multiple times: {}'.format(name, ', '.join([task.__str__() for task, _ in found])))

        if len(found) == 0:
            return None
        return found[0]

    def _get_actual_tasks(self, tasks, force_run=False):
        res = list()

        if not isinstance(tasks, list):
            tasks = [tasks]

        for item in tasks:
            if isinstance(item, TaskDefinition):
                res.append(item.create_instance(self, force_run=force_run))
            elif isinstance(item, str):
                if item in self.named_tasks:
                    res.append(self.named_tasks[item].create_instance(self, force_run=force_run))
                else:
                    task = self.lookup_task_by_pattern(item)
                    if task is None:
                        raise ValueError('No such task: {}'.format(item))
                    res.append(task[0].create_instance(self, force_run=force_run, pattern=(item, task[1])))
            else:
                raise ValueError('Invalid task: {}'.format(item))

        return res

    def execute(self, task=None, dry_run=False, force_run=False):
        if task is None:
            task = self.main

            if task is None:
                raise ValueError('No main task is set!')

        exec_nodes = list()
        for task in self._get_actual_tasks(task, force_run=force_run):
            if task[0] is not None:
                exec_nodes.append(task[0].execute())

        print('\033[1m\033[91m{}Number of tasks to run: {}\033[0m'.format('# ' if dry_run else '', len(self.task_instances)))
        executor.DRY_RUN = dry_run
        self.scheduler.execute(exec_nodes)


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
