from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import subprocess
import dask

import logging
logger = logging.getLogger(__name__)


class CliExecutor:

    def __init__(self, commands):
        self.commands = commands

    def execute(self, task_instance, dependencies=None):
        res = list()

        print('\033[1m\033[91m====================================\033[0m')
        task_instance.started()
        for command in self.commands:
            print('\033[1m\033[1;33mRunning command: \033[0m\033[0;33m{}\033[0m'.format(command))
            p = subprocess.run(args=[command], shell=True, stdout=sys.stdout, stderr=sys.stderr)
            if p.returncode != 0:
                raise RuntimeError('Failure in task: {} command: {}'.format(str(task_instance.definition), p))
            res.append(p.returncode)

        task_instance.stoped()
        return res

    def __eq__(self, other):
        if isinstance(other, CliExecutor):
            return ';'.join(self.commands) == ';'.join(other.commands)

        return False

    def __hash__(self):
        return ';'.join(self.commands).__hash__()


class DummyExecutor:

    def execute(self, dependencies=None):
        return [0]