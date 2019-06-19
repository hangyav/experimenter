from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
import sys
import signal

from experimenter.task import TaskPool

def getArguments():
  parser = argparse.ArgumentParser()

  parser.add_argument('-f', '--file', type=str, default='experiment.py', help='File that contains tasks.')
  parser.add_argument('-m', '--main', type=str, default=None, nargs='*', help='Tasks to execute.')
  parser.add_argument('-t', '--threads', type=int, default=1, help='Number of threads to Use.')
  parser.add_argument('--debug', type=int, default=0, help='Debug mode.')
  parser.add_argument('--log_level', type=str, default='WARNING', help='{NOTSET|DEBUNG|INFO|WARNING|ERROR|CRITICAL}')
  parser.add_argument('-v', '--variables', type=str, default=None, nargs='*', help='Tasks to execute.')
  parser.add_argument('-d', '--dry_run', type=int, default=0, help='Do not run any task if non-zero.')
  parser.add_argument('--force_run', type=int, default=0, help='Force running all tasks even if output exists (combine with dry_run to print commands for full experiment).')

  return parser.parse_args()




if __name__ == '__main__':
    args = getArguments()
    debug = args.debug
    log_level = args.log_level.upper()

    if log_level not in logging._nameToLevel.keys():
        sys.stderr.write('{} is not a valid logging level!\n'.format(log_level))
        sys.exit(1)

    logging.basicConfig(level=logging._nameToLevel[log_level])
    logger = logging.getLogger(__name__)

    try:
        file = args.file
        if not os.path.exists(file):
            raise ValueError('{} does not exists!'.format(file))
        file = os.path.abspath(file)

        task_pool = TaskPool.init_from_py(file, args.variables)
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

        task_pool.execute(dry_run=args.dry_run, force_run=args.force_run)

    except BaseException as e:
        if debug > 0:
            raise e
        else:
            print('\033[1m\033[91m'+ str(e) + '\033[0m')
            sys.exit(1)
