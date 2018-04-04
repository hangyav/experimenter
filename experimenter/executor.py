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

    def execute(self, dependencies=None):
        res = list()

        print('====================================')
        for command in self.commands:
            print('Running command: {}'.format(command))
            p = subprocess.run(args=[command], shell=True, stdout=sys.stdout, stderr=sys.stderr)
            if p.returncode != 0:
                raise RuntimeError('Failure in command: {}'.format(p))
            res.append(p.returncode)

        return res


class DummyExecutor:

    def execute(self, dependencies=None):
        return [0]