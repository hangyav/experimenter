from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
import sys
import signal

from experimenter.task import TaskPool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def getArguments():
  parser = argparse.ArgumentParser()

  parser.add_argument('-f', '--file', type=str, default='experiment.py', help='File that contains tasks.')
  parser.add_argument('-m', '--main', type=str, default=None, nargs='*', help='Tasks to execute.')
  parser.add_argument('-t', '--threads', type=int, default=1, help='Number of threads to Use.')
  parser.add_argument('--debug', type=int, default=0, help='Debug mode.')

  return parser.parse_args()


if __name__ == '__main__':
    debug = 0
    try:

        args = getArguments()
        debug = args.debug

        file = args.file
        if not os.path.exists(file):
            raise ValueError('{} does not exists!'.format(file))
        file = os.path.abspath(file)

        task_pool = TaskPool.init_from_py(file)
        task_pool.num_workers = args.threads

        def signal_handler(signal, frame):
            logger.error('Signal handler called with signal: {}'.format(signal))
            task_pool.handle_error()
            sys.exit(1)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        main = args.main
        if main is not None:
            task_pool.main = main

        task_pool.execute()

    except BaseException as e:
        if debug > 0:
            raise e
        else:
            print('\033[1m\033[91m'+ str(e) + '\033[0m')
            sys.exit(1)