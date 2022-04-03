
from abc import ABC, abstractmethod
from asyncio.log import logger
from concurrent.futures import thread
from logging import Logger, INFO, WARN, ERROR
from multiprocessing import Process, Queue
from multiprocessing.sharedctypes import Value
import queue
import signal
import threading
from typing import Any, Callable, Generator

import uuid

from core.commons import LoggerWrapper, WithLogging, rotary_iter
from core.extractors import AbstractExtractor
from core.loaders import AbstractLoader
from transformers import AbstractTransformer
from core.loaders import NoopLoader

QUEUE_BLOCK_TIMEOUT_SEC = 0.010
QUEUE_MAX_SIZE = 100_000

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
                max_extraction_queues: int = 5) -> None:
        super().__init__(logger)
        self.job_uuid = str(uuid.uuid1())
        self.extractor = extractor
        self.transformers = transformers
        self.loaders = loaders
        self.max_extraction_queues = max_extraction_queues
        self.pipeline_closed = Value('i', 0)
        self.extractor_finished = Value('i', 0)
        self.transformation_pipeline_alive = Value('i', 0)
        self.loaders_alive = Value('i', 0)

        if loaders is None or len(loaders)==0:
            raise RuntimeError("At least one loader is required. Or use the NoopLoader class")

        if transformers is None or len(transformers)==0:
            raise RuntimeError("At least one transformer is required. Or use the NoopTransformer class")

    @staticmethod
    def extract_items(out_queues: list[Queue], 
                        extractor: AbstractExtractor, 
                        pipeline_closed: Value, 
                        extractor_finished: Value, 
                        logger: WithLogging) -> None:
        out_queues_iter = rotary_iter(out_queues)

        for item in extractor.extract():
            if pipeline_closed.value==0:
                break
            if item is not None:
                for out_queue in out_queues_iter:
                    try:
                        out_queue.put(item, timeout=QUEUE_BLOCK_TIMEOUT_SEC)
                        break
                    except queue.Full:
                        pass
        extractor_finished.value=1
        logger.log_msg("Extractor finished his work")

    def flatMap(item:Any, mappers: list[Callable[[Any], Generator[Any, None, None]]]) -> Generator[Any, None, None]:
        if len(mappers)==0:
            yield item
        else:
            mapper = mappers[0]
            g = mapper(item)
            for x in g:
                if x is not None:
                    for a in  ThreadedPipeline.flatMap(x, mappers[1:]):
                        if a is not None:
                            yield a

    @staticmethod
    def transform_items(in_queue: Queue, 
                        out_queues: list[Queue], 
                        trans: list[AbstractTransformer], 
                        pipeline_closed: Value, 
                        extractor_finished: Value,
                        transformation_pipeline_alive: Value,
                        logger: WithLogging) -> None:
        finished = False
        while pipeline_closed.value==0:
            try:
                item = in_queue.get(timeout=QUEUE_BLOCK_TIMEOUT_SEC)
                if item is not None:
                    for x in ThreadedPipeline.flatMap(item, list(map(lambda mapper: mapper.transform, trans))):
                        if x is not None:
                            pushed_idx = {i for i in range(len(out_queues))}
                            while not pipeline_closed.value and len(pushed_idx)>0:
                                for (idx, out_queue) in enumerate(out_queues):
                                    try:
                                        if idx in pushed_idx:
                                            out_queue.put(x, timeout=QUEUE_BLOCK_TIMEOUT_SEC)
                                            pushed_idx.remove(idx)
                                    except queue.Full:
                                        pass
                        else:
                            logger.log_msg("Item found None after applying all transformers")
            except queue.Empty:
                if finished is True:
                    break
                if extractor_finished.value==1:
                    finished=True
        transformation_pipeline_alive.value -= 1
        logger.log_msg("A transformation pipeline finished her work")
    
    @staticmethod
    def load_items(job_uuid: str, 
                    out_queue: Queue, 
                    loader: AbstractLoader, 
                    pipeline_closed: Value, 
                    transformation_pipeline_alive: Value,
                    loaders_alive: Value,
                    logger: WithLogging) -> None:
        finished = False
        while pipeline_closed.value==0:
            try:
                item = out_queue.get(timeout=QUEUE_BLOCK_TIMEOUT_SEC)
                loader.load(job_uuid, [item])
            except queue.Empty:
                if finished is True:
                    break
                if transformation_pipeline_alive.value==0: # no more transformers to push data to loaders
                    finished=True
        loaders_alive.value -= 1
        logger.log_msg("A loader <{}> finished his work".format(str(loader.__class__.__name__)))
    
    def _run(self) -> None:
        extract_threads = []
        trans_threads = []
        load_threads = []
        in_queues = []
        out_queues = []
        try:
            original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGINT, original_sigint_handler)

            in_queues = [Queue(maxsize=QUEUE_MAX_SIZE) for i in range(self.max_extraction_queues)]
            out_queues = [Queue(maxsize=QUEUE_MAX_SIZE) for i in range(len(self.loaders))]

            extract_threads.append(Process(target=ThreadedPipeline.extract_items, args=(in_queues, 
                                                                                        self.extractor, 
                                                                                        self.pipeline_closed, 
                                                                                        self.extractor_finished,
                                                                                        self.logger)))                                                                            
            self.logger.log_msg("1 extraction process created", level=INFO)

            self.transformation_pipeline_alive.value = len(in_queues)
            for (idx, in_queue) in enumerate(in_queues):
                trans_threads.append(Process(target=ThreadedPipeline.transform_items, args=(in_queue, 
                                                                                            out_queues, 
                                                                                            self.transformers, 
                                                                                            self.pipeline_closed, 
                                                                                            self.extractor_finished,
                                                                                            self.transformation_pipeline_alive,
                                                                                            self.logger)))
            self.logger.log_msg("{} transformation pipelines created".format(self.transformation_pipeline_alive.value), level=INFO)

            self.loaders_alive.value = len(self.loaders)
            for (idx, out_queue) in enumerate(out_queues):
                load_threads.append(Process(target=ThreadedPipeline.load_items, args=(self.job_uuid, 
                                                                                        out_queue, 
                                                                                        self.loaders[idx], 
                                                                                        self.pipeline_closed, 
                                                                                        self.transformation_pipeline_alive,
                                                                                        self.loaders_alive,
                                                                                        self.logger)))
            self.logger.log_msg("{} loading processes created".format(len(self.loaders)), level=INFO)

            threads = load_threads +  trans_threads + extract_threads
            self.logger.log_msg("Starting {} threads of the pipeline {}.".format(len(threads), self.job_uuid), level=INFO)
            for p in threads:
                p.start()
            
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
                            t.terminate()
                    transformators_joined = True
                    self.logger.log_msg("Transformation threads joined", level=INFO)

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
                    t.kill()
                except Exception:
                    pass
            self.logger.log_msg("Threads killed", level=INFO)
            
            closables = in_queues + out_queues + self.transformers + self.loaders
            self.logger.log_msg("Closables closing ...", level=INFO)
            timer = threading.Timer(interval=1, function=lambda : thread.interrupt_main())#in seconds
            timer.start()
            try:
                for t in closables:
                    t.close()
            except Exception:
                pass
            finally:
                timer.cancel()
            self.logger.log_msg("Closables closed", level=INFO)

        finally:
            self.logger.log_msg("Pipline {} End executing".format(self.job_uuid),  level=INFO)

    def _close(self) -> None:
        self.pipeline_closed.value = 1