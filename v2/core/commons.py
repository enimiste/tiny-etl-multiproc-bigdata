from abc import ABC
import logging
from logging import ERROR, Logger
import threading
import traceback

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
        threading.Thread(target=WithLogging.log_msg_sync, args=(self.logger, msg, exception, level)).start()
    

class LoggerWrapper(WithLogging):
    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        

def rotary_iter(items: list):
    n = len(items)
    i = n-1
    while True:
        i = (i+1)%n
        yield items[i]