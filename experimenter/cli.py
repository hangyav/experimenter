from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import os
import sys
import signal

from experimenter.task import TaskPool
from experimenter import scheduler


def getArguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', '--file', type=str, default='experiment.py', help='File that contains tasks.')
    parser.add_argument('-m', '--main', type=str, default=None, nargs='*', help='Tasks to execute.')
    parser.add_argument('-t', '--threads', type=int, default=None, help='Number of threads to Use.')
    parser.add_argument('--memory', type=str, default='1000G', help='Amount of available memory.')
    parser.add_argument('--gpus', type=str, default=[], nargs='*', help='GPU indices')
    parser.add_argument('--debug', type=int, default=0, help='Debug mode.')
    parser.add_argument('--log_level', type=str, default='WARNING', help='{NOTSET|DEBUNG|INFO|WARNING|ERROR|CRITICAL}')
    parser.add_argument('-v', '--variables', type=str, default=None, nargs='*', help='Tasks to execute.')
    parser.add_argument('-d', '--dry_run', type=int, default=0, help='Do not run any task if non-zero.')
    parser.add_argument('--force_run', type=int, default=0, help='Force running all tasks even if output exists (combine with dry_run to print commands for full experiment).')
    parser.add_argument('--summarize', type=int, default=0, help='Print a summary of the tasks. Similar to --dry_run but without the commands to run.')
    parser.add_argument('--wait_for_unfinished', type=int, default=1, help='Wait for unfunished tasks upon exception.')
    parser.add_argument('-c', '--cluster', type=str, default=None, help='Use cluster')
    parser.add_argument('-cp', '--cluster_params', type=str, default=None, nargs='*', help='Cluster parameters separated by semicolon.')
    parser.add_argument('--adaptive_cluster', type=int, default=1, help='Use adaptive cluster or not.')

    return parser.parse_args()


def main():
    args = getArguments()
    debug = args.debug
    log_level = args.log_level.upper()

    if log_level not in logging._nameToLevel.keys():
        sys.stderr.write('{} is not a valid logging level!\n'.format(log_level))
        sys.exit(1)

    logging.basicConfig(level=logging._nameToLevel[log_level])
    logger = logging.getLogger(__name__)

    if args.threads is None and args.cluster is None and args.cluster_params is None:
        args.threads = 1

    try:
        file = args.file
        if not os.path.exists(file):
            raise ValueError('{} does not exists!'.format(file))
        file = os.path.abspath(file)

        sched = None
        if args.dry_run:
            sched = scheduler.LocalScheduler()
        elif args.cluster is None and args.cluster_params is None:
            if args.threads == 1:
                # TODO do this nicer
                os.environ['CUDA_VISIBLE_DEVICES'] = ','.join(args.gpus)
                sched = scheduler.LocalScheduler()
            else:
                #  sched = scheduler.LocalParallelScheduler(num_processes=args.threads,
                #                                  wait_for_unfinished=args.wait_for_unfinished !=0 )
                resource_params =[
                    f'localhost:resources:CPU:{args.threads}',
                    f'localhost:resources:MEMORY:{args.memory}',
                    'localhost:resources:GPU:{}'.format(len(args.gpus)),
                    'localhost:gpu_indices:{}'.format(','.join(args.gpus)),
                    'localhost:remote_python:{}'.format(sys.executable),
                ]
                sched = scheduler.RPyCScheduler(cluster_config=None,
                        cluster_custom_config=resource_params,
                        wait_for_unfinished=args.wait_for_unfinished != 0,
                        adaptive=args.adaptive_cluster > 0)
        else:
                sched = scheduler.RPyCScheduler(cluster_config=args.cluster,
                        cluster_custom_config=args.cluster_params,
                        wait_for_unfinished=args.wait_for_unfinished != 0,
                        adaptive=args.adaptive_cluster > 0)


        task_pool = TaskPool.init_from_py(file, sched, args.variables)

        def signal_handler(signal, frame):
            # TODO do not print exceptions in worker processes
            logger.info('Signal handler called with signal: {}'.format(signal))
            raise InterruptedError('Signal handler called with signal: {}'.format(signal))

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        main = args.main
        if main is not None:
            task_pool.main = main

        if args.summarize:
            task_pool.summarize(force_run=args.force_run)
        else:
            task_pool.execute(dry_run=args.dry_run, force_run=args.force_run)

    except BaseException as e:
        if debug > 0:
            raise e
        else:
            print('\033[1m\033[91m'+ str(e) + '\033[0m')
            sys.exit(1)


if __name__ == '__main__':
    main()
