from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
logger = logging.getLogger(__name__)


class CliExecutor:

    def __init__(self, command):
        self.command = command

    def execute(self):
        print(self.command)
        raise NotImplementedError()