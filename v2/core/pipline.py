
from abc import ABC, abstractmethod
from concurrent.futures import thread
from logging import Logger, INFO, WARN, ERROR
from multiprocessing import Process, Queue
from multiprocessing.sharedctypes import Value
import queue
import signal
from threading import Thread, Timer

import uuid

from core.commons import LoggerWrapper, WithLogging, rotary_iter
from core.extractors import AbstractExtractor
from core.loaders import AbstractLoader
from transformers import AbstractTransformer
from core.commons import flatMapApply

class AbstractPipeline(Process, ABC):
    def __init__(self, logger: Logger) -> None:
        self.logger = LoggerWrapper(logger)
        super().__init__(target=self._run)
    
    def close(self) -> None:
        self._close()
        return super().close()

    @abstractmethod
    def _run(self) -> None:
        pass

    @abstractmethod
    def _close(self)-> None:
        pass

class ThreadedPipeline(AbstractPipeline):
    def __init__(self, logger: Logger, 
                extractor: AbstractExtractor, 
                transformers: list[AbstractTransformer],
                loaders: list[AbstractLoader],
                max_transformation_pipelines: int = 5,
                use_threads_as_transformation_pipelines: bool = False,
                queue_block_timeout_sec: int = 0.1,
                queue_no_block_timeout_sec: int = 0.05,
                trans_in_queue_max_size: int = 1_000) -> None:
        super().__init__(logger)
        self.job_uuid = str(uuid.uuid1())
        self.extractor = extractor
        self.transformers = transformers
        self.loaders = loaders
        self.use_threads_as_transformation_pipelines = use_threads_as_transformation_pipelines
        self.queue_block_timeout_sec = max(0.1, queue_block_timeout_sec)
        self.queue_no_block_timeout_sec = max(0.01, queue_no_block_timeout_sec)
        self.max_transformation_pipelines = max(1, max_transformation_pipelines)
        self.trans_in_queue_max_size = max(1_000, trans_in_queue_max_size)
        self.pipeline_started = Value('i', 0)
        self.pipeline_closed = Value('i', 0)
        self.extractor_finished = Value('i', 0)
        self.transformation_pipeline_alive = Value('i', 0)
        self.loaders_alive = Value('i', 0)

        if extractor is None:
            raise RuntimeError("Extractor required")

        if loaders is None or len(loaders)==0:
            raise RuntimeError("At least one loader is required. Or use the NoopLoader class")

        if transformers is None or len(transformers)==0:
            raise RuntimeError("At least one transformer is required. Or use the NoopTransformer class")

    @staticmethod
    def extract_items(out_queues: list[Queue], 
                        extractor: AbstractExtractor, 
                        pipeline_started: Value,
                        pipeline_closed: Value, 
                        extractor_finished: Value, 
                        queue_no_block_timeout_sec: int,
                        logger: WithLogging) -> None:
        out_queues_iter = rotary_iter(out_queues)

        for item in extractor.extract():
            if pipeline_started.value==1 and pipeline_closed.value==1:
                break
            if item is not None:
                for out_queue in out_queues_iter:
                    try:
                        out_queue.put(item, timeout=queue_no_block_timeout_sec)
                        break
                    except queue.Full:
                        pass
        extractor_finished.value=1
        logger.log_msg("Extractor finished his work", level=INFO)

    @staticmethod
    def transform_items(in_queue: Queue, 
                        out_queues: list[Queue], 
                        trans: list[AbstractTransformer], 
                        pipeline_started: Value,
                        pipeline_closed: Value, 
                        extractor_finished: Value,
                        transformation_pipeline_alive: Value,
                        queue_block_timeout_sec: int,
                        queue_no_block_timeout_sec: int,
                        logger: WithLogging) -> None:
        finished = False
        while pipeline_closed.value==0:
            try:
                item = in_queue.get(timeout=queue_block_timeout_sec)
                context = {}
                if item is not None:
                    for x in flatMapApply(item, list(map(lambda mapper: mapper.transform, trans)), context=context):
                        if x is not None:
                            pushed_idx = {i for i in range(len(out_queues))}
                            while not pipeline_closed.value and len(pushed_idx)>0:
                                for (idx, out_queue) in enumerate(out_queues):
                                    try:
                                        if idx in pushed_idx:
                                            out_queue.put(x, timeout=queue_no_block_timeout_sec)
                                            pushed_idx.remove(idx)
                                    except queue.Full:
                                        pass
                        else:
                            logger.log_msg("Item found None after applying all transformers")
            except queue.Empty:
                if finished is True:
                    break
                if extractor_finished.value==1:
                    finished=pipeline_started.value==1
        transformation_pipeline_alive.value -= 1
        logger.log_msg("A transformation pipeline finished her work", level=INFO)
    
    @staticmethod
    def load_items(job_uuid: str, 
                    out_queue: Queue, 
                    loader: AbstractLoader, 
                    pipeline_started: Value,
                    pipeline_closed: Value, 
                    transformation_pipeline_alive: Value,
                    loaders_alive: Value,
                    queue_block_timeout_sec: int,
                    logger: WithLogging) -> None:
        finished = False
        ack_counter = Value('i', 0)
        while pipeline_closed.value==0:
            try:
                item = out_queue.get(timeout=queue_block_timeout_sec)
                ack_counter.value += 1
                loader.loadWithAck(job_uuid, [item], ack_counter, last_call=finished)
            except queue.Empty:
                if finished is True and ack_counter.value==0:
                    logger.log_msg("Closing loader <{}>".format(str(loader.__class__.__name__)), level=INFO)
                    loader.close()
                    break
                if transformation_pipeline_alive.value==0: # no more transformers to push data to loaders
                    finished=pipeline_started.value==1
        loaders_alive.value -= 1
        logger.log_msg("A loader <{}> finished his work".format(str(loader.__class__.__name__)), level=INFO)
    
    def _run(self) -> None:
        extract_threads = []
        trans_threads = []
        load_threads = []
        in_queues = []
        out_queues = []
        try:
            self.pipeline_started.value=0
            self.pipeline_closed.value=0

            original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGINT, original_sigint_handler)

            in_queues = [Queue(maxsize=self.trans_in_queue_max_size) for i in range(self.max_transformation_pipelines)]
            out_queues = [Queue(maxsize=(self.trans_in_queue_max_size * self.max_transformation_pipelines)) for i in range(len(self.loaders))]
            
            extract_threads.append(Process(target=ThreadedPipeline.extract_items, args=(in_queues, 
                                                                                        self.extractor, 
                                                                                        self.pipeline_started,
                                                                                        self.pipeline_closed, 
                                                                                        self.extractor_finished,
                                                                                        self.queue_no_block_timeout_sec,
                                                                                        self.logger)))                                                                            
            self.logger.log_msg("1 extraction process created", level=INFO)

            self.transformation_pipeline_alive.value = self.max_transformation_pipelines
            for (idx, in_queue) in enumerate(in_queues):
                params = {
                    'target': ThreadedPipeline.transform_items, 
                    'args': (in_queue, 
                            out_queues, 
                            self.transformers, 
                            self.pipeline_started,
                            self.pipeline_closed, 
                            self.extractor_finished,
                            self.transformation_pipeline_alive,
                            self.queue_block_timeout_sec,
                            self.queue_no_block_timeout_sec,
                            self.logger)
                }
                if self.use_threads_as_transformation_pipelines:
                    p = Thread(target=params["target"], args=params["args"])
                else:
                    p = Process(target=params["target"], args=params["args"])

                trans_threads.append(p)
            self.logger.log_msg("{} transformation pipelines created".format(self.transformation_pipeline_alive.value), level=INFO)

            self.loaders_alive.value = len(self.loaders)
            for (idx, out_queue) in enumerate(out_queues):
                load_threads.append(Process(target=ThreadedPipeline.load_items, args=(self.job_uuid, 
                                                                                        out_queue, 
                                                                                        self.loaders[idx], 
                                                                                        self.pipeline_started,
                                                                                        self.pipeline_closed, 
                                                                                        self.transformation_pipeline_alive,
                                                                                        self.loaders_alive,
                                                                                        self.queue_block_timeout_sec,
                                                                                        self.logger)))
            self.logger.log_msg("{} loading processes created".format(len(self.loaders)), level=INFO)

            threads = load_threads +  trans_threads + extract_threads
            self.logger.log_msg("Starting {} threads of the pipeline {}.".format(len(threads), self.job_uuid), level=INFO)
            for p in threads:
                p.start()
            self.pipeline_started.value=1
            self.logger.log_msg("Pipeline {} running".format(self.job_uuid), level=INFO)

            extractor_joined=False
            transformators_joined=False
            loaders_joined=False
            while self.pipeline_closed.value==0:
                if not extractor_joined and self.extractor_finished.value==1:
                    for t in extract_threads:
                        if self.pipeline_closed.value==0:
                            t.join()
                    extractor_joined=True
                    self.logger.log_msg("Extract threads joined", level=INFO)

                if not transformators_joined and  self.transformation_pipeline_alive.value==0:
                    for t in trans_threads:
                        if self.pipeline_closed.value==0:
                            t.join()
                    transformators_joined = True
                    self.logger.log_msg("Transformation threads joined. Waiting for loaders to finish their words...", level=INFO)

                if not loaders_joined and self.loaders_alive.value==0:
                    for t in load_threads:
                        if self.pipeline_closed.value==0:
                            t.join()
                    loaders_joined = True
                    self.logger.log_msg("Load threads joined", level=INFO)

                if extractor_joined and transformators_joined and loaders_joined:
                    self.close()

        except KeyboardInterrupt:
            self.logger.log_msg("Caught KeyboardInterrupt, terminating workers ...", level=INFO)
            self.close()

            threads = extract_threads + trans_threads + load_threads
            self.logger.log_msg("Threads killing ...", level=INFO)
            for t in threads:
                try:
                    if type(t) is Thread:
                        t._stop()
                    else:
                        t.kill()
                except Exception:
                    pass
            self.logger.log_msg("Threads killed", level=INFO)
        
        finally:
            queues = in_queues + out_queues
            self.logger.log_msg("Queues closing ...", level=INFO)
            timer = Timer(interval=1, function=lambda : thread.interrupt_main())#in seconds
            timer.start()
            try:
                for q in queues:
                    q.close()
            except Exception:
                pass
            finally:
                timer.cancel()
            self.logger.log_msg("Queues closed", level=INFO)
            self.logger.log_msg("Pipline {} End executing".format(self.job_uuid),  level=INFO)

    def _close(self) -> None:
        self.pipeline_closed.value = 1