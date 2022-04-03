from abc import ABC
from functools import reduce
import logging
from logging import ERROR, Logger
import threading
import traceback
from typing import Any, Callable, Generator

class WithLogging(ABC):
    def __init__(self, logger: Logger) -> None:
        self.logger = logger

    @staticmethod
    def log_msg_sync(logger: Logger, msg: str, exception: Exception = None, level: int = logging.DEBUG):
        if not logger is None:
            if not exception is None and level==ERROR:
                logger.log(level, "{}, Trace : {}".format(msg, str(traceback.format_exception(exception))))
            else:
                logger.log(level, msg)
        else:
            print(msg)
        
    def log_msg(self, msg: str, exception: Exception = None, level: int = logging.DEBUG):
        pass
        # threading.Thread(target=WithLogging.log_msg_sync, args=(self.logger, msg, exception, level)).start()
    

class LoggerWrapper(WithLogging):
    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        

def rotary_iter(items: list):
    n = len(items)
    i = n-1
    while True:
        i = (i+1)%n
        yield items[i]

def dict_deep_get(dictionary: dict, keys: list[str]):
    return reduce(lambda d, key: d.get(key) if (type(d) is dict and key in d) else None, keys, dictionary)

def dict_deep_set(dictionary: dict, keys: list[str], value):
    if len(keys)==0:
        return
    container = reduce(lambda d, key: d.get(key) if (type(d) is dict and key in d) else {}, keys[:-1], dictionary)
    container[keys[-1]] = value

def dict_deep_remove(dictionary: dict, keys: list[str]):
    if keys is not None:
        if len(keys)==0:
            return
        container = dict_deep_get(dictionary, keys[:-1])
        if container is not None and keys[-1] in container:
            del container[keys[-1]]

def flatMapApply(item:Any, mappers: list[Callable[[Any], Generator[Any, None, None]]], **kwargs) -> Generator[Any, None, None]:
        if len(mappers)==0:
            yield item
        else:
            mapper = mappers[0]
            g = mapper(item, **kwargs)
            for x in g:
                if x is not None:
                    for a in  flatMapApply(x, mappers[1:], **kwargs):
                        if a is not None:
                            yield a