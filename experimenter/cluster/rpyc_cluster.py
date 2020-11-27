from __future__ import absolute_import, division, print_function

import sys
import argparse
import logging
import math
import threading
from collections import defaultdict
from concurrent.futures import _base
from multiprocessing import Lock

import rpyc
import dill
import yaml
from plumbum import SshMachine
from rpyc.utils.zerodeploy import DeployedServer

logger = logging.getLogger(__name__)


CPU = 'CPU'
MEMORY = 'MEMORY'
GPU = 'GPU'
GPU_INDICES = 'gpu_indices'


def task1():
    import subprocess
    p = subprocess.Popen(args=['python -c \'import torch; print(torch.cuda.device_count())\''], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #  p = subprocess.Popen(args=['nvidia-smi'], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()
    print(output)
    return output


def get_resources(gpu_exclude=None, gpu_max_load=0.05, gpu_max_memory=0.05, gpu_memory_free=0,
                  cpu_load_idx=1):
    import GPUtil
    import psutil

    resources = dict()

    num_cpus = psutil.cpu_count()
    load = int(psutil.getloadavg()[cpu_load_idx])
    resources['CPU'] = num_cpus - min(num_cpus, load)

    resources['MEMORY'] = psutil.virtual_memory().available

    if gpu_exclude is None:
        gpu_exclude = list()
    gpus = GPUtil.getAvailable(maxLoad=gpu_max_load, maxMemory=gpu_max_memory,
                               excludeID=gpu_exclude, memoryFree=gpu_memory_free,
                               limit=1000000)
    resources['GPU'] = gpus
    print(resources)
    return resources


def run_dill_encoded(what):
    import dill
    fun, args = dill.loads(what)
    return fun(*args)


class WorkItem():

    def __init__(self, function, resources, future):
        self.function = function
        self.resources = resources
        self.future = future
        #
        self.connection = None
        self.async_result = None
        self.used_resources = None


class RPyCConnection():

    def __init__(self, connection, server, server_name, ssh):
        self.connection = connection
        self.server = server
        self.server_name = server_name
        self.ssh = ssh

    def __del__(self):
        self.server.close()

    @property
    def modules(self):
        return self.connection.modules

    @property
    def teleport(self):
        return self.connection.teleport


class RPyCCluster(_base.Executor):

    def __init__(self, cluster_config_file=None, cluster_custom_config=None):
        self.cluster_config = RPyCCluster.load_config(cluster_config_file, cluster_custom_config)
        self.used_resources = defaultdict(dict)
        self.lock = Lock()
        self.pending_work_items = list()
        self.running_work_items = list()
        self.sentry_thread = None
        self.new_work_item_event = threading.Event()

    @staticmethod
    def load_config(file=None, custom=None):
        """
        param: custom is : separated items, e.g. localhost:gpu_indices:1,2,4
        """

        assert file is not None or custom is not None, 'Either cluster file or parameters must be given'

        if file is not None:
            with open(file, 'r') as fin:
                res = yaml.load(fin)
        else:
            res = dict()

        if custom is not None:
            for items in custom:
                items = items.split(':')
                if ',' in items[-1]:
                    tmp = items[-1].split(',')
                    for i in range(len(tmp)):
                        try:
                            tmp[i] = int(tmp[i])
                        except:
                            pass
                    items[-1] = tmp
                else:
                    try:
                        items[-1] = int(items[-1])
                    except:
                        pass

                curr_res = res
                for i in items[:-2]:
                    curr_res = curr_res.setdefault(i, dict())
                curr_res[items[-2]] = items[-1]

        mem_convert = ['K', 'M', 'G', 'T']
        for server in res.keys():
            for resource, value in res[server].setdefault('resources', dict()).items():
                if resource == MEMORY and type(value) == str and value[-1] in mem_convert:
                    res[server]['resources'][MEMORY] = int(float(value[:-1]) * math.pow(1024, mem_convert.index(value[-1])+1))

        return res

    def get_connection_for_resource(self, resources):
        selected_res = None
        selected_server = None
        with self.lock:
            available_res = self.get_available_resources()
            for server, res in available_res.items():
                selected_res = RPyCCluster.get_resource_setup(resources, res)
                if selected_res is not None:
                    selected_server = server
                    break;
            else:
                # TODO handle case when there is no enough resources for the taks in the whole cluster
                pass

            if selected_server is not None:
                self.add_used(selected_server, selected_res)

        if selected_server is not None:
            return self.get_connection(selected_server, selected_res), selected_res

        return None



    def get_available_resources(self):
        res = defaultdict(dict)

        for server, params in sorted(self.cluster_config.items(), reverse=True,
                                     key=lambda x:x[1].setdefault('priority', 0)):
            for resource, amount in params['resources'].items():
                res[server][resource] = amount - self.used_resources.setdefault(
                                                server, dict()
                                                ).setdefault(resource, 0)

            res[server][GPU_INDICES] = sorted(
                set(params.setdefault(GPU_INDICES, list()))
                - self.used_resources.setdefault(server, dict()).setdefault(GPU_INDICES, set())
            )

        return res

    @staticmethod
    def get_resource_setup(needed_resources, available_resources):
        res = dict()
        for resource, value in needed_resources.items():
            if value > available_resources.setdefault(resource, 0):
                return None
            else:
                if resource == GPU and value > len(available_resources.setdefault(GPU_INDICES, list())):
                    return None
                else:
                    res[GPU_INDICES] = available_resources[GPU_INDICES][:value]

                res[resource] = value

        return res

    def add_used(self, server, resources):
        for resource, value in resources.items():
            if resource == GPU_INDICES:
                self.used_resources[server].setdefault(resource, set()).update(value)
            else:
                self.used_resources[server].setdefault(resource, 0)
                self.used_resources[server][resource] += value

    def free_up_resources(self, server, resources):
        for resource, value in resources.items():
            try:
                if resource == GPU_INDICES:
                    self.used_resources[server][resource] -= set(value)
                else:
                    self.used_resources[server][resource] -= value
            except BaseException as e:
                print(server, resource, value, self.used_resources)
                raise e

    def get_connection(self, server, resources):
        logger.info('Initializing connection to {}...'.format(server))
        m = SshMachine(server,
                       user=self.cluster_config[server].setdefault('ssh_username', None),
                       port=self.cluster_config[server].setdefault('ssh_port', None),
                       keyfile=self.cluster_config[server].setdefault('ssh_private_key', None),
                       password=self.cluster_config[server].setdefault('ssh_password', None))

        cmd = self.cluster_config[server].setdefault('remote_python', None)
        setup = ''
        if GPU_INDICES in resources and len(resources[GPU_INDICES]) > 0:
            setup = 'os.environ[\'CUDA_VISIBLE_DEVICES\'] = \'{}\''.format(','.join([str(i) for i in resources[GPU_INDICES]]))

        logger.info('Command: {}, Setup: {}'.format(cmd, setup))

        s = DeployedServer(m, python_executable=cmd, server_class='rpyc.utils.server.OneShotServer',
                           extra_setup=setup)
        c = s.classic_connect()

        connection = RPyCConnection(c, s, server, m)

        return connection


    def submit(self, fn, resources, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        f = _base.Future()
        w = WorkItem(fn, resources, f)
        self.pending_work_items.append(w)

        self.start_sentry_thread()
        self.new_work_item_event.set()

        return f

    def shutdown(self, wait=True):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Args:
            wait: If True then shutdown will not return until all running
                futures have finished executing and the resources used by the
                executor have been reclaimed.
        """
        # TODO
        print('TODO SHUTDOWN!!!!')


    def start_sentry_thread(self):
        if self.sentry_thread is None:
            self.sentry_thread = threading.Thread(target=self.sentry_fn)
            self.sentry_thread.daemon = True
            self.sentry_thread.start()

    def sentry_fn(self):
        task_idx = 0
        while True:
            # clean up finished items
            to_pop = list()
            for i, item in enumerate(self.running_work_items):
                if item.async_result.ready:
                    if item.async_result.error:
                        try:
                            item.async_result.value
                        except BaseException as e:
                            item.future.set_exception(e)
                    else:
                        item.future.set_result(item.async_result.value)

                    self.free_up_resources(item.connection.server_name, item.used_resources)
                    item.connection.server.close()
                    to_pop.append(i)

            for i in to_pop[::-1]:
                self.running_work_items.pop(i)

            # Start new tasks
            to_pop = list()
            for i, item in enumerate(self.pending_work_items):
                conn_obj = self.get_connection_for_resource(item.resources)

                if conn_obj is not None:
                    connection = conn_obj[0]
                    resources = conn_obj[1]
                    self.start_process_monitor_thread(connection, task_idx)
                    task_idx +=1

                    fn = connection.teleport(run_dill_encoded)
                    res = rpyc.async_(fn)(dill.dumps((item.function, [])))

                    item.connection = connection
                    item.used_resources = resources
                    item.async_result = res

                    to_pop.append(i)
                    self.running_work_items.append(item)


            for i in to_pop[::-1]:
                self.pending_work_items.pop(i)


            self.new_work_item_event.wait(1)
            self.new_work_item_event.clear()

    @staticmethod
    def monitor_process(info, inp):
        for line in inp:
            print(f'{info}: '.format(), line.decode('utf-8').strip())

    @staticmethod
    def start_process_monitor_thread(connection, task_idx):
        # STDOUT
        thread = threading.Thread(target=RPyCCluster.monitor_process, args=(
            '{}-{}-{}'.format(connection.server_name, task_idx, 'OUT'),
            connection.server.proc.stdout,)
            )
        thread.daemon = True
        thread.start()
        # STDERR
        thread = threading.Thread(target=RPyCCluster.monitor_process, args=(
            '{}-{}-{}'.format(connection.server_name, task_idx, 'ERR'),
            connection.server.proc.stderr,)
            )
        thread.daemon = True
        thread.start()


def getArguments():
  parser = argparse.ArgumentParser()

  #  parser.add_argument('-s', '--server', type=str, help='Server to connect to.')
  #  parser.add_argument('-f', '--file', type=str, default='experiment.py', help='File that contains tasks.')
  #  parser.add_argument('-m', '--main', type=str, default=None, nargs='*', help='Tasks to execute.')
  #  parser.add_argument('-t', '--threads', type=int, default=1, help='Number of threads to Use.')
  #  parser.add_argument('--debug', type=int, default=0, help='Debug mode.')
  #  parser.add_argument('--log_level', type=str, default='WARNING', help='{NOTSET|DEBUNG|INFO|WARNING|ERROR|CRITICAL}')
  #  parser.add_argument('-v', '--variables', type=str, default=None, nargs='*', help='Tasks to execute.')
  #  parser.add_argument('-d', '--dry_run', type=int, default=0, help='Do not run any task if non-zero.')
  #  parser.add_argument('--force_run', type=int, default=0, help='Force running all tasks even if output exists (combine with dry_run to print commands for full experiment).')
  #  parser.add_argument('--wait_for_unfinished', type=int, default=1, help='Wait for unfunished tasks upon exception.')
  parser.add_argument('-c', '--cluster', type=str, default=None,  help='Cluster definition file')
  parser.add_argument('-cp', '--cluster_params', type=str, default=None, nargs='*', help='Cluster parameters separated by semicolon.')

  return parser.parse_args()


if __name__ == '__main__':
    args = getArguments()
    logging.basicConfig(level=logging.INFO)

    #  from rpyc.utils.server import ThreadedServer
    #  t = ThreadedServer(RPyCCluster(), port=18861)
    #  t.start()

    #  m = SshMachine('localhost')
    #  m = SshMachine(args.server)
    #  s = DeployedServer(m, python_executable='~/.anaconda/bin/python')
    #  c = s.classic_connect()

    cluster = RPyCCluster(cluster_config_file=args.cluster, cluster_custom_config=args.cluster_params)
    #
    #  c = cluster.get_connection_for_resource({CPU: 1})
    c, _ = cluster.get_connection_for_resource({CPU: 1, GPU: 3})
    print(c.modules.sys.executable)
    print(c.modules.os.environ)
    res = c.teleport(get_resources)
    t = c.teleport(task1)

    print('Resources: ', res())
    print('Task1: ', t().decode('utf-8'))
