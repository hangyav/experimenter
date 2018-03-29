from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import re
import os
from experimenter.executor import CliExecutor


logger = logging.getLogger(__name__)

class TaskDefinition:

    def __init__(self, actions, name=None, params=None, patterns=None, dependencies=None, executor=CliExecutor,
                 outputs=None):
        if len(actions) == 0 and len(dependencies) == 0:
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
            for name, value in match.groupdict().items():
                params[name] = value

        ##########################################################
        dependencies = list()
        latest_parent_modification = -1.0
        if self.dependencies is not None:
            for dep in self.dependencies:
                dep = dep.format(**params)
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
        tasks = list()
        if self.actions is not None:
            for task in self.actions:
                tasks.append(self.executor(task.format(**params)))

        outputs = list()
        if self.outputs is not None:
            outputs = [o.format(**params) for o in self.outputs]

        earliest_modification = self._earliest_output_modification_time(outputs)
        if len(dependencies) > 0:
            return (TaskInstance(tasks, dependencies, self), self._latest_output_modification_time(outputs))
        elif self.dependencies is not None and len(self.dependencies) > 0 and len(dependencies) == 0 \
                and len(outputs) > 0 and self._is_output_exists(outputs) and latest_parent_modification > -1.0 and latest_parent_modification <= earliest_modification:
            logger.info('All dependencies are satisfied. Ignoring task.')
            return (None, self._latest_output_modification_time(outputs))
        elif self.dependencies is None and len(outputs) > 0 and self._is_output_exists(outputs):
            logger.info('All outputs are satisfied. Ignoring task.')
            return (None, self._latest_output_modification_time(outputs))

        return (TaskInstance(tasks, dependencies, self), self._latest_output_modification_time(outputs))

    @staticmethod
    def _is_output_exists(outputs):
        if outputs is None or len(outputs) == 0:
            return True

        for o in outputs:
            if not os.path.exists(o):
                return False

        return True

    @staticmethod
    def _output_modification_time(outputs, func):
        if outputs is None or len(outputs) == 0:
            return None

        o = outputs[0]
        res = None
        if os.path.exists(o):
            res = os.path.getmtime(o)
        for o in outputs[1:]:
            if os.path.exists(o):
                res = func(res, os.path.getmtime(o))

        return res

    @staticmethod
    def _earliest_output_modification_time(outputs):
        return TaskDefinition._output_modification_time(outputs, min)

    @staticmethod
    def _latest_output_modification_time(outputs):
        return TaskDefinition._output_modification_time(outputs, max)


class TaskInstance:

    def __init__(self, tasks, dependencies, definition):
        assert len(tasks) > 0 or len(dependencies) > 0, 'Error no task nor dependency for task instance!'

        self.tasks = tasks
        self.dependecies = dependencies
        self.definition = definition

    def execute(self):

        for dep in self.dependecies:
            dep.execute()

        print('====================================')
        for task in self.tasks:
            task.execute()


class TaskPool:

    def __init__(self, tasks, main=None):
        self.tasks = tasks
        self.named_tasks = {task.name: task for task in tasks if task.name is not None}
        self.main = main

        self._patterns = list()
        for task in tasks:
            if task.patterns is None:
                continue

            for dep in task.patterns:
                self._patterns.append((re.compile('^{}$'.format(dep)), task))

    @staticmethod
    def init_from_py(file):
        import sys
        if sys.version_info[0] == 2:
            importer = execFile
        else:
            importer = exec

        with open(file, 'r') as fin:
            importer(fin.read())

        tasks = [var for name, var in locals().items() if isinstance(var, TaskDefinition)]
        main_task = None
        if 'main' in locals():
            main_task = locals()['main']
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
                res.append(item)
            elif isinstance(item, str):
                res.append(self.named_tasks[item])
            else:
                raise ValueError('Invalid task: {}'.format(item))

        return res

    def execute(self, task=None):
        if task is None:
            task = self.main

            if task is None:
                raise ValueError('No main task is set!')

        for t in self._get_actual_tasks(task):
            task = t.create_instance(self)
            if task is not None:
                task[0].execute()