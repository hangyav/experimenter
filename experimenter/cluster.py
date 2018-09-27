from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from distributed.deploy import Adaptive
from distributed import Scheduler
from tornado.ioloop import IOLoop
from distributed.cli.utils import install_signal_handlers
from distributed.utils import log_errors


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
            self.max_resources[CPU] = self.default_cpu
        if GPU not in self.max_resources:
            self.max_resources[GPU] = self.default_gpu
        if MEMORY not in self.max_resources:
            self.max_resources[MEMORY] = self.default_memory

    def get_scale_up_kwargs(self):
        restrictions = [self.get_restrictions(t[0]) for t in self.scheduler.tasks.items() if t[1].state == 'no-worker']
        restrictions = sorted(restrictions, key=lambda x: (x[GPU], x[CPU], x[MEMORY]), reverse=True)
        res = list()
        gpu = self.max_resources[GPU]
        cpu = self.max_resources[CPU]
        memory = self.max_resources[MEMORY]

        for rest in restrictions:
            if rest[GPU] <= gpu and rest[CPU] <= cpu and rest[MEMORY] <= memory:
                res.append(rest)
                gpu -= rest[GPU]
                cpu -= rest[CPU]
                memory -= rest[MEMORY]

        return {'params':res}

    def get_restrictions(self, id):
        restriction = self.scheduler.resource_restrictions[id]
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
        # get all workers, filter out those which are suitable for corrent tasks, return (close) the rest

        #  if len(self.scheduler.workers) <= self.minimum:
        #      return []
        #
        #  kw = dict(self._workers_to_close_kwargs)
        #  kw.update(kwargs)
        #
        #  if self.maximum is not None and len(self.scheduler.workers) > self.maximum:
        #      kw['n'] = len(self.scheduler.workers) - self.maximum
        #
        #  L = self.scheduler.workers_to_close(**kw)
        #  if len(self.scheduler.workers) - len(L) < self.minimum:
        #      L = L[:len(self.scheduler.workers) - self.minimum]
        #
        #  return L

        return []


class SSHCluster(object):
    def scale_up(self, params):
        print('ADD: ', params)
        #  raise NotImplementedError()

    def scale_down(self, workers):
        print('REMOVE: ', workers)
        #  raise NotImplementedError()

if __name__ == '__main__':
    loop = IOLoop.current()
    scheduler = Scheduler(loop=loop)
    cluster = SSHCluster()
    adapative_cluster = ResourceAwareAdaptive(scheduler, cluster=cluster, minimum=0, maximum=10)
    scheduler.start()
    install_signal_handlers(loop)

    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()
