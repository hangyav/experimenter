from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from concurrent.futures import ProcessPoolExecutor
from threading import Event
from experimenter.cluster.rpyc_cluster import RPyCCluster

import logging
logger = logging.getLogger(__name__)


class LocalScheduler():

    def __init__(self):
        self.running_nodes = set()
        self.finished_nodes = set()

    def execute(self, nodes):
        nodes = set(nodes)

        while not nodes.issubset(self.finished_nodes):
            runnable_nodes = set()
            for node in nodes:
                runnable_nodes.update(self._get_runnable_nodes(node))

            for node in runnable_nodes:
                node.function()
                self.finished_nodes.add(node)


    def _get_runnable_nodes(self, node):
        res = set()

        if node in self.running_nodes or node in self.finished_nodes:
            return res

        if node.dependencies is None or len(node.dependencies) == 0:
            res.add(node)
        else:
            for dep in node.dependencies:
                if dep not in self.finished_nodes:
                    break;
            else:
                res.add(node)

            if len(res) == 0:
                for dep in node.dependencies:
                    res.update(self._get_runnable_nodes(dep))

        return res


class LocalParallelScheduler(LocalScheduler):

    def __init__(self, num_processes, wait_for_unfinished=True):
        super().__init__()
        assert num_processes > 1

        self.num_processes = num_processes
        self.wait_for_unfinished = wait_for_unfinished

    def execute(self, nodes):
        nodes = set(nodes)

        with ProcessPoolExecutor(self.num_processes) as pool:
            exception = False
            futures = list()
            event = Event()
            def callback(future):
                event.set()

            try:
                while not nodes.issubset(self.finished_nodes):
                    runnable_nodes = set()
                    for node in nodes:
                        runnable_nodes.update(self._get_runnable_nodes(node))

                    event.clear()
                    for node in runnable_nodes:
                        self.running_nodes.add(node)
                        f = pool.submit(fn=node.function)
                        f.add_done_callback(callback)
                        futures.append((f, node))

                    while len([f for f in futures if not f[0].done()]) >= self.num_processes:
                        event.wait(10)
                        event.clear()

                    futures_tmp = futures
                    futures = list()
                    for f, n in futures_tmp:
                        if f.done():
                            self.running_nodes.remove(n)
                            f.result()
                            self.finished_nodes.add(n)
                        else:
                            futures.append((f, n))
                    # XXX the line below seems to cause a deadlock when
                    # all tasks are finished and running with multiple processes
                    # the above seems to work OK
                    #  futures = [item for item in futures if not item[0].done()]

            except Exception as e:
                exception = True
                raise e
            finally:
                if exception:
                    if self.wait_for_unfinished:
                        logger.warning('Waiting for running tasks to finish after exception...')
                    else:
                        for pid, p in pool._processes.items():
                            p.terminate()
                while len([f for f in futures if not f[0].done()]) > 0:
                    event.wait(10)
                    event.clear()


class RPyCScheduler(LocalScheduler):

    def __init__(self, cluster_config, wait_for_unfinished=True):
        super().__init__()

        self.cluster_config = cluster_config
        self.wait_for_unfinished = wait_for_unfinished

    def execute(self, nodes):
        nodes = set(nodes)

        with RPyCCluster(self.cluster_config) as pool:

            exception = False
            futures = list()
            event = Event()

            try:
                while not nodes.issubset(self.finished_nodes):
                    runnable_nodes = set()
                    for node in nodes:
                        runnable_nodes.update(self._get_runnable_nodes(node))

                    event.clear()
                    for node in runnable_nodes:
                        self.running_nodes.add(node)
                        f = pool.submit(fn=node.function, resources=node.resources)
                        futures.append((f, node))

                    futures_tmp = futures
                    futures = list()
                    for f, n in futures_tmp:
                        if f.done():
                            self.running_nodes.remove(n)
                            f.result()
                            self.finished_nodes.add(n)
                        else:
                            futures.append((f, n))
                    # XXX the line below seems to cause a deadlock when
                    # all tasks are finished and running with multiple processes
                    # the above seems to work OK
                    #  futures = [item for item in futures if not item[0].done()]

            except Exception as e:
                exception = True
                raise e
            finally:
                if exception:
                    if self.wait_for_unfinished:
                        logger.warning('Waiting for running tasks to finish after exception...')
                    else:
                        pool.shutdown()
                while len([f for f in futures if not f[0].done()]) > 0:
                    event.wait(10)
                    event.clear()
