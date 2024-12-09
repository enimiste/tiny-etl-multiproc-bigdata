from abc import abstractmethod
from logging import INFO, WARN, ERROR, Logger, DEBUG
from multiprocessing.sharedctypes import Value
from typing import AnyStr, List, Set, Tuple
import uuid

from tiny_etl.commons import WithLogging
from tiny_etl.commons import dict_deep_get
 
class AbstractLoader(WithLogging):
    def __init__(self, logger: Logger, 
                input_key_path: List[AnyStr],
                values_path: List[Tuple[str, List[AnyStr], bool]]) -> None:
        super().__init__(logger)
        self.input_key_path = input_key_path
        self.values_path = values_path
        self.uuid = str(uuid.uuid1())
        
    def loadWithAck(self, job_uuid: str, items: List[dict], ack_counter: Value, last_call: bool) -> None:
        try:
            self.load(job_uuid, items, last_call)
        finally:
            ack_counter.value -= 1
            

    @abstractmethod
    def load(self, job_uuid: str, items: List[dict], last_call: bool) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    def has_buffered_data(self) -> bool:
        return False

    def kill_threads_processes(self):
        pass

class NoopLoader(AbstractLoader):
    def __init__(self, logger, 
                input_key_path: List[AnyStr],
                values_path: List[Tuple[str, List[AnyStr], bool]] = [],
                log: bool = False,
                log_level=DEBUG) -> None:
        super().__init__(logger, input_key_path, values_path)
        self.log = log
        self.log_level = log_level

    def load(self, job_uuid: str, items: List[dict], last_call: bool) -> None:
        if self.log:
            for item in items:
                super().log_msg("NoopLoader <Item loaded> : {}".format(str(self._row_from_data(dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item))), level=self.log_level)

    def _row_from_data(self, item: dict)->list:
        row = []
        for (title, key_path, required) in self.values_path:
            val = dict_deep_get(item, key_path)
            if required is not None and required is True and val is None:
                return None
            row.append(val)
        return row

    def close(self) -> None:
        pass

class ConditionalLoader(AbstractLoader):
    def __init__(self, 
                    logger,
                    condition, 
                    wrapped_loader: AbstractLoader, 
                    else_log: bool = False) -> None:
        super().__init__(logger, None, None)
        self.condition = condition
        self.wrapped_loader = wrapped_loader
        self.else_log = else_log

    def check_condition(self, items=None):
        if callable(self.condition):
            return self.condition(items)
        else:
            return self.condition

    def loadWithAck(self, job_uuid: str, items: List[dict], ack_counter: Value, last_call: bool) -> None:
        if self.check_condition():
            return self.wrapped_loader.loadWithAck(job_uuid, items, ack_counter, last_call)
        elif self.else_log:
            super().log_msg("Item loaded : {}".format(str(items)))
        ack_counter.value -= 1

    def load(self, job_uuid: str, items: List[dict], last_call: bool) -> None:
        if self.check_condition(items):
            return self.wrapped_loader.load(job_uuid, items, last_call)
        elif self.else_log:
            super().log_msg("Item loaded : {}".format(str(items)))

    def close(self) -> None:
        if self.check_condition():
            return self.wrapped_loader.close()

    def has_buffered_data(self) -> bool:
        if self.check_condition():
            return self.wrapped_loader.has_buffered_data()
        else:
            return super().has_buffered_data()

    def kill_threads_processes(self):
        if self.check_condition():
            return self.wrapped_loader.kill_threads_processes()

