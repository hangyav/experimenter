from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from distributed import Scheduler
from distributed.deploy import Adaptive
from distributed.deploy.ssh import async_ssh
from tornado.ioloop import IOLoop
from distributed.cli.utils import install_signal_handlers
from distributed.utils import log_errors
import toolz
import getpass
import os
import sys
from threading import Thread
import time
import yaml

try:
    from queue import Queue
except ImportError:  # Python 2.7 fix
    from Queue import Queue


logger = logging.getLogger(__name__)

CPU = 'CPU'
GPU = 'GPU'
MEMORY = 'MEMORY'

class ResourceAwareAdaptive(Adaptive):

    def __init__(self, scheduler, max_resources=None, default_cpu=1,
                 default_memory=1024, default_gpu=0, **kwargs):
        super().__init__(scheduler, **kwargs)
        self.max_resources = max_resources if max_resources is not None else dict()
        self.default_cpu = default_cpu
        self.default_memory = default_memory
        self.default_gpu = default_gpu

        if CPU not in self.max_resources:
            self.max_resources[CPU] = -1
        if GPU not in self.max_resources:
            self.max_resources[GPU] = -1
        if MEMORY not in self.max_resources:
            self.max_resources[MEMORY] = -1

    def get_used_resources(self):
        resources = {CPU:0, GPU:0, MEMORY:0}

        for worker in self.scheduler.workers.values():
            for res, val in worker.resources.items():
                if res not in resources:
                    resources[res] = val
                else:
                    resources[res] += val

        return resources

    def get_scale_up_kwargs(self):
        restrictions = [self.get_restrictions(t[0]) for t in self.scheduler.tasks.items() if t[1].state == 'no-worker']
        restrictions = sorted(restrictions, key=lambda x: (x[GPU], x[CPU], x[MEMORY]), reverse=True)
        res = list()
        used_resources = self.get_used_resources()
        gpu = self.max_resources[GPU] - used_resources[GPU]
        cpu = self.max_resources[CPU] - used_resources[CPU]
        memory = self.max_resources[MEMORY] - used_resources[MEMORY]

        for rest in restrictions:
            if (gpu < 0 or rest[GPU] <= gpu) and (cpu < 0 or rest[CPU] <= cpu) \
                    and (memory < 0 or rest[MEMORY] <= memory):
                res.append(rest)
                gpu -= rest[GPU]
                cpu -= rest[CPU]
                memory -= rest[MEMORY]

        return {'params':res}

    def get_restrictions(self, id):
        restriction = self.scheduler.resource_restrictions[id] if id in self.scheduler.resource_restrictions else {}
        if CPU not in restriction:
            restriction[CPU] = self.default_cpu
        if MEMORY not in restriction:
            restriction[MEMORY] = self.default_memory
        if GPU not in restriction:
            restriction[GPU] = self.default_gpu

        return restriction

    def get_required_resources(self):
        tasks = [t for t in self.scheduler.tasks.items() if t[1].state == 'no-worker']

        required_resources = dict()
        for tid, task in tasks:
            restriction = self.get_restrictions(tid)
            for res, val in restriction.items():
                if res not in required_resources:
                    required_resources[res] = val
                else:
                    required_resources[res] += val

        return required_resources


    def should_scale_up(self):
        with log_errors():
            required_resources = self.get_required_resources()

            for res, val in required_resources.items():
                if res in self.max_resources:
                    required_resources[res] = min(val, self.max_resources[res])

            used_resources = dict()
            for worker in self.scheduler.workers.values():
                for res, val in worker.resources.items():
                    if res not in used_resources:
                        used_resources[res] = val
                    else:
                        used_resources[res] += val

            for res, val in required_resources.items():
                if res not in used_resources:
                    return True
                elif required_resources[res] > used_resources[res]:
                    return True

            return False

    def workers_to_close(self, **kwargs):
        restrictions = [self.get_restrictions(t[0]) for t in self.scheduler.tasks.items() if t[1].state == 'no-worker']
        restrictions = sorted(restrictions, key=lambda x: (x[GPU], x[CPU], x[MEMORY]), reverse=True)
        # get all workers, filter out those which are suitable for corrent tasks, return (close) the rest
        workers = [w for w in self.scheduler.workers.values() if len(w.processing) == 0]

        tmp_workers = workers.copy()
        for rest in restrictions:
            for w in tmp_workers:
                for res, val in rest.items():
                    if res not in w.resources or w.resources[res] < val:
                        break
                else:
                    workers.remove(w)

        workers = [w.address for w in workers]

        return workers

    def recommendations(self, comm=None):
        should_scale_up = self.should_scale_up()
        workers = set(self.workers_to_close(key=self.worker_key,
                                            minimum=self.minimum))

        if should_scale_up:
            self.close_counts.clear()
            return toolz.merge({'status': 'up'}, self.get_scale_up_kwargs())
        elif workers:
            d = {}
            to_close = []
            for w, c in self.close_counts.items():
                if w in workers:
                    if c >= self.wait_count:
                        to_close.append(w)
                    else:
                        d[w] = c

            for w in workers:
                d[w] = d.get(w, 0) + 1

            self.close_counts = d

            if to_close:
                return {'status': 'down', 'workers': to_close}
        elif should_scale_up:
            self.close_counts.clear()
            return toolz.merge({'status': 'up'}, self.get_scale_up_kwargs())
        else:
            self.close_counts.clear()
            return None


class SSHCluster(object):

    def __init__(self, scheduler, cluster_config):
        self.scheduler = scheduler
        self.staring = set()
        self.processes = dict()
        self.cluster_config = SSHCluster.load_config(cluster_config)

        self.monitor_thread = Thread(target=self.monitor)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    @staticmethod
    def load_config(file):
        with open(file, 'r') as fin:
            res = yaml.load(fin)
        res = sorted(res.items(), key=lambda x:x[1]['priority'])
        return res

    @staticmethod
    def is_fit(resources, requirements, used_resources=None):
        if used_resources is None:
            used_resources = dict()
        for res, val in requirements.items():
            if (res not in resources and val > 0) or resources.get(res, 0)-used_resources.get(res, 0) < val:
                return False
        return True

    def get_used_resources(self, node): # TODO live monitor
        result = dict()

        for h, process in self.processes.items():
            if process['address'] == node:
                if process['gpu_indices'] is not None:
                    if 'gpu_indices' in result:
                        result['gpu_indices'] = result['gpu_indices'].union(set(process['gpu_indices']))
                    else:
                        result['gpu_indices'] = set(process['gpu_indices'])
                for res, val in process['resources'].items():
                    result[res] = result.get(res, 0) + val

        return result

    def get_config_for_request(self, requirements):
        for node, config in self.cluster_config:
            used_resources = self.get_used_resources(node)
            if SSHCluster.is_fit(config['resources'], requirements, used_resources):
                result = config.copy()
                result['resources'] = requirements
                result['worker_address'] = node

                if GPU in result['resources'] and result['resources'][GPU] > 0:
                    result['gpu_indices'] = list(
                        set(result['gpu_indices']) - set(used_resources.get('gpu_indices', set()))
                    )[:result['resources'][GPU]]

                    if result['resources'][GPU] > len(result['gpu_indices']):
                        result['resources'][GPU] = len(result['gpu_indices'])
                    result['resources']['GPU_{}'.format(result['resources'][GPU])] = 1 # do not waste GPUs

                return result

        return None

    def scale_up(self, params):
        for p in params:
            p = {k:v for k, v in p.items() if 'GPU_' not in k} # do not waste GPUs
            h = str(p)
            if h in self.staring:
                continue
            config = self.get_config_for_request(p)
            if config is None:
                logger.info('No available resources for: {}'.format(p))
                continue

            self.staring.add(h)
            self.processes[h] = self.start_worker(**config)

    def scale_down(self, workers):
        pass

    def monitor(self):
        while True:
            for h, process in self.processes.items():
                while not process['output_queue'].empty():
                    line = process['output_queue'].get()
                    if 'Starting established connection' in line:
                        self.staring.remove(h)
                    print(line)
            time.sleep(0.1)

    def start_worker(self, worker_address, resources,
                     logdir=None, nthreads=1, nprocs=1, ssh_username=None,
                     ssh_port=22, ssh_private_key=None, nohost=True,
                     memory_limit=None, worker_port=None, nanny_port=None,
                     remote_python=None, gpu_indices=None, **kwargs):

        scheduler_addr = self.scheduler.ip
        scheduler_port = self.scheduler.port

        cmd = ('{params} {python} -m distributed.cli.dask_worker '
               '{scheduler_addr}:{scheduler_port} '
               '--nthreads {nthreads} --nprocs {nprocs} --resources {resources}')

        if not nohost:
            cmd += ' --host {worker_address} '

        if memory_limit:
            cmd += '--memory-limit {memory_limit} '

        if worker_port:
            cmd += '--worker-port {worker_port} '

        if nanny_port:
            cmd += '--nanny-port {nanny_port} '

        cmd = cmd.format(
            python=remote_python or sys.executable,
            scheduler_addr=scheduler_addr,
            scheduler_port=scheduler_port,
            worker_address=worker_address,
            nthreads=nthreads,
            nprocs=nprocs,
            memory_limit=memory_limit,
            worker_port=worker_port,
            nanny_port=nanny_port,
            resources=','.join(['{}={}'.format(res, val) for res, val in resources.items()]),
            params='CUDA_VISIBLE_DEVICES={}'.format(','.join([str(i) for i in gpu_indices])) if gpu_indices is not None else ''
        ).strip()

        # Optionally redirect stdout and stderr to a logfile
        if logdir is not None:
            cmd = 'mkdir -p {logdir} && '.format(logdir=logdir) + cmd
            cmd += '&> {logdir}/dask_scheduler_{addr}.log'.format(
                addr=worker_address, logdir=logdir)

        label = '{} {}'.format(worker_address, resources)

        if ssh_username is None:
            ssh_username = getpass.getuser()
        if ssh_private_key is None:
            ssh_private_key = os.path.expanduser('~/.ssh/id_rsa.pub')

        # Create a command dictionary, which contains everything we need to run and
        # interact with this command.
        input_queue = Queue()
        output_queue = Queue()
        cmd_dict = {'cmd': cmd, 'label': label, 'address': worker_address,
                    'input_queue': input_queue, 'output_queue': output_queue,
                    'ssh_username': ssh_username, 'ssh_port': ssh_port,
                    'ssh_private_key': ssh_private_key}

        def thread_wrapper(h, label, cmd_dict):
            try:
                async_ssh(cmd_dict)
            except OSError as e:
                pass

            print('[ {} ] : {}'.format(label, 'Closed!'))
            del self.processes[h]

        # Start the thread
        thread = Thread(target=thread_wrapper, args=[str(resources), label, cmd_dict])
        thread.daemon = True
        thread.start()

        return toolz.merge(cmd_dict, {'thread': thread, 'resources': resources, 'gpu_indices': gpu_indices})



if __name__ == '__main__':
    loop = IOLoop.current()
    scheduler = Scheduler(loop=loop)
    cluster = SSHCluster(scheduler, 'cluster.yml')
    adapative_cluster = ResourceAwareAdaptive(scheduler, cluster=cluster, max_resources={CPU:10, MEMORY:10000, GPU:8})
    scheduler.start()
    install_signal_handlers(loop)

    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()
