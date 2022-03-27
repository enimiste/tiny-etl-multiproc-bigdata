
from logging import Logger, INFO, WARN, ERROR
from multiprocessing import Process, Queue
from multiprocessing.sharedctypes import Value
import queue
import signal
from threading import Thread

import uuid

from core.commons import WithLogging, rotary_iter
from core.extractors import AbstractExtractor
from core.loaders import AbstractLoader
from transformers import AbstractTransformer



class AbstractPipeline(WithLogging):
    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)

    def close(self) -> None:
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
        self.closed=Value('i', 0)

    @staticmethod
    def extract_items(out_queues: list[Queue], extractor: AbstractExtractor, closed: Value) -> None:
        out_queues_iter = rotary_iter(out_queues)

        for item in extractor.extract():
            if item is not None:
                for out_queue in out_queues_iter:
                    try:
                        out_queue.put(item, timeout=0.1)
                        break
                    except queue.Empty:
                        pass

    @staticmethod
    def transform_items(in_queue: Queue, out_queues: list[Queue], trans: list[AbstractTransformer], closed: Value) -> None:
        while closed.value==0:
            try:
                item = in_queue.get(block=False, timeout=0.1)
                if item is not None:
                    for tra in trans:
                        item = tra.transform(item)
                    pushed_idx = {i for i in range(len(out_queues))}
                    while not closed.value and len(pushed_idx)>0:
                        for (idx, out_queue) in enumerate(out_queues):
                            try:
                                if idx in pushed_idx:
                                    out_queue.put(item, block=False, timeout=1)
                                    pushed_idx.remove(idx)
                            except queue.Empty:
                                pass
            except queue.Empty:
                pass
    
    @staticmethod
    def load_items(job_uuid: str, out_queue: Queue, loader: AbstractLoader, closed: Value) -> None:
        while closed.value==0:
            try:
                item = out_queue.get(block=False, timeout=0.1)
                loader.load(job_uuid, [item])
            except queue.Empty:
                pass
    
    def run(self) -> None:
        extract_threads = []
        trans_threads = []
        load_threads = []
        in_queues = []
        out_queues = []
        try:
            original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGINT, original_sigint_handler)

            in_queues = [Queue(maxsize=100_000) for i in range(self.max_extraction_queues)]
            out_queues = [Queue(maxsize=100_000) for i in range(len(self.loaders))]

            extract_threads.append(Process(target=ThreadedPipeline.extract_items, args=(in_queues, self.extractor, self.closed)))
            for in_queue in in_queues:
                trans_threads.append(Thread(target=ThreadedPipeline.transform_items, args=(in_queue, out_queues, self.transformers, self.closed)))

            for (idx, out_queue) in enumerate(out_queues):
                load_threads.append(Process(target=ThreadedPipeline.load_items, args=(self.job_uuid, out_queue, self.loaders[idx], self.closed)))

            threads = load_threads +  trans_threads + extract_threads
            self.log_msg("Starting {} threads of the pipeline {}.".format(len(threads), self.job_uuid), level=INFO)
            for p in threads:
                p.start()
            
            self.log_msg("Pipeline {} running".format(self.job_uuid), level=INFO)

            while self.closed.value==0:
                for t in extract_threads:
                    t.join()
                self.log_msg("Extract threads joined", level=INFO)
                
                
                while len([x for x in filter(lambda x: x.qsize()>0, in_queues)])>0:
                    pass
                self.log_msg("IN Queues joined", level=INFO)

                while len([x for x in filter(lambda x: x.qsize()>0, out_queues)])>0:
                    pass
                self.log_msg("OUT Queues joined", level=INFO)

                self.close()

                for t in trans_threads:
                    t.join()
                self.log_msg("Transformation threads joined", level=INFO)

                for t in load_threads:
                    t.join()
                self.log_msg("Load threads joined", level=INFO)


        except KeyboardInterrupt:
            self.log_msg("Caught KeyboardInterrupt, terminating workers ...", level=INFO)
            self.close()
            all = extract_threads + trans_threads + load_threads + in_queues + out_queue + self.transformers + self.loaders
            for t in all:
                try:
                    t.close()
                except Exception:
                    pass
        finally:
            self.log_msg("Pipline {} End executing".format(self.job_uuid),  level=INFO)

    def close(self) -> None:
        self.closed.value = 1