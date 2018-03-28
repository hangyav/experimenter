from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import re
from experimenter.executor import CliExecutor


logger = logging.getLogger(__name__)

class TaskDefinition:

    def __init__(self, actions, name=None, params=None, patterns=None, dependencies=None, executor=CliExecutor):
        if len(actions) == 0 and len(dependencies) == 0:
            raise ValueError('At least one action or one dependencie has to be defined!')

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

    def create_instance(self, pool, pattern=None):
        params = self.params
        if params is None:
            params = dict()

        if pattern is not None and pattern[1] is not None:
            match = re.search(pattern[1], pattern[0])
            for name, value in match.groupdict().items():
                params[name] = value


        dependencies = list()
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
                    # TODO handle file deps without tasks
                    raise NotImplementedError()

                dependencies.append(dep_task.create_instance(pool, pattern=(dep, pattern)))

        tasks = list()
        if self.actions is not None:
            for task in self.actions:
                tasks.append(self.executor(task.format(**params)))

        return TaskInstance(tasks, dependencies)

class TaskInstance:

    def __init__(self, tasks, dependencies):
        assert len(tasks) > 0 or len(dependencies) > 0, 'Error no task nor dependency for task instance!'

        self.tasks = tasks
        self.dependecies = dependencies

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
                self._patterns.append((re.compile(dep), task))

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
            raise ValueError('{} matched multiple times: {}'.format(name, ', '.join([task.name for task in found])))

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
            t.create_instance(self).execute()