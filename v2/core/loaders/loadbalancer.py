from logging import INFO, WARN, ERROR, Logger, DEBUG
import multiprocessing
from multiprocessing.sharedctypes import Value
import queue
from typing import AnyStr, List, Set, Tuple

from core.commons import WithLogging
from core.commons import rotary_iter
from core.commons import block_join_threads_or_processes
from core.commons import LoggerWrapper
from core.commons import make_thread_process
from core.commons import set_process_affinity
from core.loaders.commons import AbstractLoader


class LoadBalanceLoader(AbstractLoader):
    def __init__(self, 
                    logger,
                    loaders: List[Tuple[int, AbstractLoader]],
                    cpus_affinity_options: List[int],
                    buffer_size: int = 1000,
                    queue_no_block_timeout_sec: int = 0.09,
                    queue_block_timeout_sec: int = 0.1,
                    use_threads_as_loaders_executors: bool = True) -> None:
        super().__init__(logger, None, None)
        self.loaders = loaders
        self.cpus_affinity_options = set(cpus_affinity_options)
        self.started = False
        self.buffer_size=buffer_size
        self.buffer = []
        self.ack_dec = 0
        self.queues = []
        self.rotary_iter_queues = []
        self.queue_no_block_timeout_sec=max(0.01, queue_no_block_timeout_sec)
        self.queue_block_timeout_sec = max(0.1, queue_block_timeout_sec)
        self.use_threads_as_loaders_executors = use_threads_as_loaders_executors
        self.load_balancer_closed= Value('i', 0)
        self.loaders_threads = []

        if len(loaders)<=1:
            raise RuntimeError('At least two loaders should be passed to the load balancer')
        if len(cpus_affinity_options)==0:
            raise RuntimeError('Cpu affinity options should be not empty')

    @staticmethod
    def load_items(idx: int,
                    job_uuid: str, 
                    in_queue: multiprocessing.Queue, 
                    loader: AbstractLoader, 
                    queue_block_timeout_sec: int,
                    load_balancer_closed: Value,
                    logger: WithLogging) -> None:
        finished = False
        while True:
            try:
                (last_call, items) = in_queue.get(timeout=queue_block_timeout_sec)
                loader.load(job_uuid, items, last_call=finished or last_call)
            except queue.Empty:
                if finished:
                    break
            finally:
                finished=load_balancer_closed.value==1
        if logger is not None:
            logger.log_msg("Loader N° {} in the Loadbalancer <{}> stopped".format(idx, loader.__class__.__name__), level=INFO)

    def start_loadbalancer(self, job_uuid: str):
        self.queues = [multiprocessing.Queue(maxsize=max(100, q_size)) for (q_size, _) in self.loaders]
        self.rotary_iter_queues = rotary_iter(self.queues)
        for (idx, queue_) in enumerate(self.queues):
                params = {
                    'target':LoadBalanceLoader.load_items, 
                    'args': (idx,
                            job_uuid, 
                            queue_, 
                            self.loaders[idx][1], 
                            self.queue_block_timeout_sec,
                            self.load_balancer_closed, 
                            LoggerWrapper(self.logger))
                }
                self.loaders_threads.append(make_thread_process(self.use_threads_as_loaders_executors, 
                                                                params["target"], 
                                                                params["args"]))
        threads_started_count = 0
        for t in self.loaders_threads:
            t.start()
            threads_started_count+=1
            set_process_affinity(t, self.cpus_affinity_options, log_prefix='Loader balancer loaders', print_log=True)
        super().log_msg('{}/{} threads started for loadbalancing'.format(threads_started_count, len(self.loaders_threads)), level=INFO)
        self.started=True

    def loadWithAck(self, job_uuid: str, items: List[dict], ack_counter: Value, last_call: bool) -> None:
        self.load(job_uuid, items, last_call, ack_counter)

    def load(self, job_uuid: str, items: List[dict], last_call: bool, ack_counter: Value=None) -> None:
        if not self.started:
            self.start_loadbalancer(job_uuid)
            self.started=True

        if len(items) >0: 
            self.buffer = self.buffer + items
            self.ack_dec += len(items)

        if last_call or len(self.buffer) >= self.buffer_size:
            self.balance(ack_counter)
            self.load_balancer_closed.value = 1 if last_call else 0


    def balance(self, ack_counter: Value=None, last_call: bool = False):
        for queue_ in self.rotary_iter_queues:
            try:
                queue_.put((last_call, [] + self.buffer), timeout=self.queue_no_block_timeout_sec)
                break
            except queue.Full:
                pass
        if ack_counter is not None:
            ack_counter.value -= self.ack_dec
        self.clear_buffer_and_ack()
        

    def close(self) -> None:
        super().log_msg("Closing the Loadbalancer <{}> ...".format(str(self.__class__.__name__)), level=INFO)
        try:
            if self.has_buffered_data():
                super().log_msg('Flushing buffered data in the LoadBalancer <{}>'.format(str(self.__class__.__name__)), level=INFO)
                self.balance(last_call=True)
                self.clear_buffer_and_ack()
                super().log_msg('Flushed buffered data in the LoadBalancer <{}>'.format(str(self.__class__.__name__)), level=INFO)
        except Exception as ex:
            raise ex

        super().log_msg('Joining loaders threads in the LoadBalancer <{}>'.format(str(self.__class__.__name__)), level=INFO)
        self.load_balancer_closed.value=1
        block_join_threads_or_processes(self.loaders_threads, ignore_exception=False)

        super().log_msg('Closing loaders in the LoadBalancer <{}>'.format(str(self.__class__.__name__)), level=INFO)
        for (_, loader) in self.loaders:
            try:
                loader.close()
            except Exception :
                pass

        super().log_msg('Closing queues in the LoadBalancer <{}>'.format(str(self.__class__.__name__)), level=INFO)
        for q in self.queues:
            try:
                q.close()
                q.cancel_join_thread()
            except Exception :
                pass
        self.started=False
        self.loaders_threads.clear()

    def clear_buffer_and_ack(self):
        self.buffer.clear()
        self.ack_dec = 0

    def has_buffered_data(self) -> bool:
        return len(self.buffer)>0

    def kill_threads_processes(self):
        if len(self.loaders_threads) > 0:
            block_join_threads_or_processes(self.loaders_threads, ignore_exception=False)
            self.loaders_threads.clear()

