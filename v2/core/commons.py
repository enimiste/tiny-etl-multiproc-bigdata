from abc import ABC
from functools import reduce
import logging
from logging import ERROR, Logger, DEBUG
import multiprocessing
import os
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
        WithLogging.log_msg_sync(self.logger, msg, exception, level)
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

def get_thread_process_id(th):
    if isinstance(th, threading.Thread):
        return th.ident
    elif isinstance(th, multiprocessing.Process):
        return th.pid
    raise RuntimeError('Invalid thread/process object {}'.format(type(th)))

def get_thread_process_is_joined(th) -> bool:
    if isinstance(th, threading.Thread):
        return not th.is_alive()
    elif isinstance(th, multiprocessing.Process):
        return th.exitcode is not None
    raise RuntimeError('Invalid thread/process object {}'.format(type(th)))

def kill_threads_processes(threads: list[Any], ignore_exception:bool=True):
    for th in threads:
        kill_thread_process(th, ignore_exception)

def kill_thread_process(th, ignore_exception:bool=True):
    try:
        if isinstance(th, threading.Thread):
            th._stop()
            return
        elif isinstance(th, multiprocessing.Process):
            th.kill()
            return
    except Exception as ex:
        if not ignore_exception:
            raise ex
    raise RuntimeError('Invalid thread/process object {}'.format(type(th)))

def terminate_thread_process(th, ignore_exception:bool=True):
    try:
        if isinstance(th, threading.Thread):
            th._stop()
            return
        elif isinstance(th, multiprocessing.Process):
            th.terminate()
            return
    except Exception as ex:
        if not ignore_exception:
            raise ex
    raise RuntimeError('Invalid thread/process object {}'.format(type(th)))

def block_join_threads_or_processes(threads: list[Any], 
                                    interrupt_on: Callable[[], bool] = None, 
                                    join_timeout: int = 0.01, 
                                    ignore_exception:bool=True,
                                    logger: Logger = None,
                                    log_when_joined: bool=False,
                                    log_msg: str=None,
                                    log_level=DEBUG) -> bool:
    nbr_threads = len(threads)
    if nbr_threads==0:
        return
    joined_ids = set()
    while len(joined_ids) < nbr_threads:
        for t in threads:
            try:
                t_id = get_thread_process_id(t)
                if t_id not in joined_ids:
                    if interrupt_on is not None and interrupt_on():
                        joined_ids.add(t_id) # should be before terminate()
                        if log_when_joined and logger is not None:
                            logger.log(log_level, log_msg)
                        kill_thread_process(t)
                    else:
                        t.join(timeout=join_timeout)
                        is_joined = get_thread_process_is_joined(t)
                        if is_joined:
                            joined_ids.add(t_id)
                            if log_when_joined and logger is not None:
                                logger.log(log_level, log_msg)
                            terminate_thread_process(t)
            except Exception as ex:
                if not ignore_exception:
                    raise ex
    return True


def basename_backwards(path: str, backwards_level: int=2) -> str:
    backwards_level = max(2, backwards_level)
    paths = []
    while backwards_level>0:
        if len(path)==0 or path == '.' or path=='..':
            break
        paths.append(os.path.basename(path))
        path = os.path.dirname(path)
        backwards_level-=1

    if len(paths)==0:
        return path
    else:
        paths.reverse()
        return os.path.join(paths[0], *paths[1:])
    
def make_thread_process(use_thread: bool, target, args) -> threading.Thread | multiprocessing.Process:
    if use_thread:
        return threading.Thread(target=target, args=args)
    else:
        return multiprocessing.Process(target=target, args=args)

def get_dir_size_in_mo(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size/1024/1024