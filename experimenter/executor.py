from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import subprocess
import logging
logger = logging.getLogger(__name__)

DRY_RUN = False

class CliExecutor:

    def __init__(self, commands):
        self.commands = commands

    def execute(self, task_instance):
        res = list()

        print('\033[1m\033[91m{}================={}===================\033[0m'.format('# ' if DRY_RUN else '',
                                        ' {} '.format(task_instance.definition.name) if task_instance.definition.name else ''))
        try:
            for command in self.commands:
                command = command.strip()
                print_cmd = True
                if command[0] == '@':
                    command = command[1:]
                    print_cmd = DRY_RUN
                if print_cmd:
                    print('\033[1m\033[1;33m{}\033[0m\033[0;33m{}\033[0m'.format('' if DRY_RUN else 'Running command: ', command))
                if not DRY_RUN:
                    p = subprocess.run(args=[command], shell=True, stdout=sys.stdout, stderr=sys.stderr)
                    if p.returncode != 0:
                        raise RuntimeError('Failure in task: {} command: {}'.format(str(task_instance.definition), p))
                    res.append(p.returncode)
                else:
                    res.append(0)

            return res
        except Exception as e:
            task_instance.recover()
            raise e

    def __eq__(self, other):
        if isinstance(other, CliExecutor):
            return ';'.join(self.commands) == ';'.join(other.commands)

        return False

    def __hash__(self):
        return ';'.join(self.commands).__hash__()


class DummyExecutor:

    def execute(self, dependencies=None):
        return [0]
