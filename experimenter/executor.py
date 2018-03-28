from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import subprocess

import logging
logger = logging.getLogger(__name__)


class CliExecutor:

    def __init__(self, command):
        self.command = command

    def execute(self):
        print('Running command: {}'.format(self.command))
        subprocess.run(args=[self.command], shell=True, stdout=sys.stdout, stderr=sys.stderr)