from __future__ import absolute_import, division, print_function

import sys
import argparse
import logging
import math
import threading
from collections import defaultdict
from concurrent.futures import _base
from multiprocessing import Lock, Process, Queue

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
LOCAL = 'LOCAL'
LOCAL_SERVER_NAMES = ['localhost', '127.0.0.1']


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
    return resources


MEM_CONVERT = ['K', 'M', 'G', 'T']

def canonicalize_resources(resources):
    for resource, value in resources.items():
        if resource == MEMORY and type(value) == str and value[-1] in MEM_CONVERT:
            resources[MEMORY] = int(float(value[:-1]) * math.pow(1024, MEM_CONVERT.index(value[-1])+1))


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
        self.process = None
        self.async_result = None

class ProcessConnection():

    def __init__(self, server_name, task_id, resources):
        self.server_name = server_name
        self.task_id = task_id
        self.resources = resources

    def monitor_process(self, inp, stream):
        for line in inp:
            print('{}-{}-{}: '.format(self.server_name, self.task_id, stream), line.decode('utf-8').strip())

    def start_process_monitor_thread(self):
        # STDOUT
        thread = threading.Thread(target=self.monitor_process,
            args=(self.stdout, 'OUT'),
        )
        thread.daemon = False
        thread.start()
        # STDERR
        thread = threading.Thread(target=self.monitor_process,
            args=(self.stderr, 'ERR'),
        )
        thread.daemon = False
        thread.start()

    @property
    def stdout(self):
        raise NotImplementedError

    @property
    def stderr(self):
        raise NotImplementedError

    def run_work_item(self, item):
        raise NotImplementedError

    def stop(self):
        pass

    def __del__(self):
        self.stop()


class QueueWrapper():

    def __init__(self):
        self.queue = Queue()

    def write(self, obj):
        print('write')
        self.queue.put_nowait(obj)

    def __iter__(self):
        while True:
            yield self.queue.get(True)

    def fileno(self):
        print('fileno')
        return 0

    def close(self):
        print('close')
        return

    def read(self):
        print('read')
        return ''


class LocalProcess(ProcessConnection):

    def __init__(self, server_name, task_id, resources):
        super().__init__(server_name, task_id, resources)

        self.process = None
        self.out = QueueWrapper()
        self.err = QueueWrapper()
        self.result = None

    def stop(self):
        if self.process is not None:
            self.process.terminate()

    @property
    def stdout(self):
        return self.out

    @property
    def stderr(self):
        return self.err

    @property
    def ready(self):
        return not self.process.is_alive()

    @property
    def value(self):
        return self.result

    @property
    def error(self):
        return isinstance(self.result, Exception)

    def run_work_item(self, item):
        self.process = Process(target=self.local_process_wrapper, args=(item.function, ))
        self.process.start()
        #  self.start_process_monitor_thread()

        item.process = self
        item.async_result = self

    def local_process_wrapper(self, func):
        # TODO this doesn't work
        #  import sys
        #  sys.stdout = self.stdout
        #  sys.stderr = self.stderr
        if GPU_INDICES in self.resources:
                import os
                os.environ['CUDA_VISIBLE_DEVICES'] = ','.join(map(str, self.resources[GPU_INDICES]))

        try:
            self.result = func()
        except BaseException as e:
            self.results = e

class RPyCConnection(ProcessConnection):

    def __init__(self, server_name, task_id, resources, cluster_config):
        super().__init__(server_name, task_id, resources)

        logger.info('Initializing connection to {}...'.format(server_name))
        self.ssh = SshMachine(server_name,
                       user=cluster_config[server_name].setdefault('ssh_username', None),
                       port=cluster_config[server_name].setdefault('ssh_port', None),
                       keyfile=cluster_config[server_name].setdefault('ssh_private_key', None),
                       password=cluster_config[server_name].setdefault('ssh_password', None))

        cmd = cluster_config[server_name].setdefault('remote_python', None)
        setup = ''
        if GPU_INDICES in resources and len(resources[GPU_INDICES]) > 0:
            setup = 'os.environ[\'CUDA_VISIBLE_DEVICES\'] = \'{}\''.format(','.join([str(i) for i in resources[GPU_INDICES]]))

        logger.info('Command: {}, Setup: {}'.format(cmd, setup))

        self.server = DeployedServer(self.ssh, python_executable=cmd, server_class='rpyc.utils.server.OneShotServer',
                           extra_setup=setup)
        self.connection = self.server.classic_connect()

    def stop(self):
        self.connection.close()
        self.server.close()
        self.ssh.close()

    @property
    def modules(self):
        return self.connection.modules

    @property
    def teleport(self):
        return self.connection.teleport

    @property
    def stdout(self):
        return self.server.proc.stdout

    @property
    def stderr(self):
        return self.server.proc.stderr

    def run_work_item(self, item):
        self.start_process_monitor_thread()

        func = self.connection.teleport(run_dill_encoded)
        result = rpyc.async_(func)(dill.dumps((item.function, [])))

        item.process = self
        item.async_result = result


class RPyCCluster(_base.Executor):

    def __init__(self, cluster_config_file=None, cluster_custom_config=None):
        self.cluster_config = RPyCCluster.load_config(cluster_config_file, cluster_custom_config)
        logger.info(f'Available resource on the cluster: {self.cluster_config}')
        self.used_resources = defaultdict(dict)
        self.lock = Lock()
        self.pending_work_items = list()
        self.running_work_items = list()
        self.sentry_thread = None
        self.new_work_item_event = threading.Event()
        self.shutdown_flag = 0
        self.num_overall_connections = 0

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

        for server in res.keys():
            if server in LOCAL_SERVER_NAMES:
                # TODO consider using higher number because this way only 1
                # process with LOCAL=1 can run
                res[server].setdefault('resources', dict())[LOCAL] = 1

            canonicalize_resources(res[server].setdefault('resources', dict()))
            if GPU_INDICES in res[server] and type(res[server][GPU_INDICES]) != list:
                try:
                    l = len(res[server][GPU_INDICES])
                    if l == 0:
                        res[server][GPU_INDICES] = []
                    else:
                        res[server][GPU_INDICES] = [res[server][GPU_INDICES]]
                except TypeError:
                    res[server][GPU_INDICES] = [res[server][GPU_INDICES]]



        return res

    def get_connection_for_resource(self, resources):
        logger.info(f'Looking for a node for: {resources}')
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
            return self.get_connection(selected_server, selected_res)

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
        if server in LOCAL_SERVER_NAMES or LOCAL in resources:
            connection = LocalProcess(server, self.num_overall_connections, resources)
        else:
            connection = RPyCConnection(server, self.num_overall_connections, resources, self.cluster_config)
        self.num_overall_connections += 1

        return connection


    def submit(self, fn, resources, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs) and returns
        a Future instance representing the execution of the callable.

        Returns:
            A Future representing the given call.
        """
        if self.shutdown_flag > 0:
            raise Exception('Shutdown aleady initiated')

        f = _base.Future()
        w = WorkItem(fn, resources, f)
        self.pending_work_items.append(w)

        self.start_sentry_thread()
        self.new_work_item_event.set()

        return f

    def shutdown(self, wait=True, force=False):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Args:
            wait: If True then shutdown will not return until all running
                futures have finished executing and the resources used by the
                executor have been reclaimed.
        """
        self.shutdown_flag = 2 if force else 1
        self.new_work_item_event.set()
        if wait:
            while True:
                if len(self.running_work_items) + len(self.pending_work_items) == 0:
                    return

                self.new_work_item_event.wait()


    def start_sentry_thread(self):
        if self.sentry_thread is None:
            self.sentry_thread = threading.Thread(target=self.sentry_fn)
            self.sentry_thread.daemon = False
            self.sentry_thread.start()

    def sentry_fn(self):
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

                    self.free_up_resources(item.process.server_name, item.process.resources)
                    item.process.stop()
                    to_pop.append(i)
                elif self.shutdown_flag > 1:
                    # TODO connection is not interrupted
                    item.future.set_exception(InterruptedError('Interrupted by user'))
                    self.free_up_resources(item.process.server_name, item.process.resources)
                    item.process.stop()
                    to_pop.append(i)

            for i in to_pop[::-1]:
                self.running_work_items.pop(i)

            # Start new tasks
            to_pop = list()
            for i, item in enumerate(self.pending_work_items):
                if self.shutdown_flag > 1:
                    to_pop.append(i)
                    continue

                connection = self.get_connection_for_resource(item.resources)

                if connection is not None:
                    connection.run_work_item(item)
                    to_pop.append(i)
                    self.running_work_items.append(item)

            for i in to_pop[::-1]:
                self.pending_work_items.pop(i)

            if self.shutdown_flag > 0 and len(self.running_work_items) + len(self.pending_work_items) == 0:
                break

            self.new_work_item_event.wait(1)
            self.new_work_item_event.clear()

        self.new_work_item_event.set()


def getArguments():
  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--cluster', type=str, default=None,  help='Cluster definition file')
  parser.add_argument('-cp', '--cluster_params', type=str, default=None, nargs='*', help='Cluster parameters separated by semicolon.')

  return parser.parse_args()


if __name__ == '__main__':
    args = getArguments()
    logging.basicConfig(level=logging.INFO)


    cluster = RPyCCluster(cluster_config_file=args.cluster, cluster_custom_config=args.cluster_params)
    c = cluster.get_connection_for_resource({CPU: 1, GPU: 3})
    print(c.modules.sys.executable)
    print(c.modules.os.environ)
    res = c.teleport(get_resources)

    print('Resources: ', res())
