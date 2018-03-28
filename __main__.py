from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os

from experimenter.task import TaskPool, TaskDefinition

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def getArguments():
  parser = argparse.ArgumentParser()

  parser.add_argument('-f', '--file', type=str, default='experiment.py', help='File that contains tasks.')

  return parser.parse_args()


if __name__ == '__main__':
    # try:

    args = getArguments()

    file = args.file
    if not os.path.exists(file):
        raise ValueError('{} does not exists!'.format(file))
    file = os.path.abspath(file)

    task_pool = TaskPool.init_from_py(file)
    task_pool.execute()

    # except BaseException as e:
    #     print(e)