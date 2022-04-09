from abc import ABC, abstractmethod
from functools import reduce
import logging
from logging import ERROR, Logger, DEBUG, INFO
import math
import multiprocessing
import os
import random
import sys
import threading
import traceback
from typing import Any, AnyStr, Callable, Generator, List, Set, Tuple
import uuid

from core.affinity import set_process_affinity_mask

def rotary_iter(items: list, rand: bool=False):
    random.seed(str(uuid.uuid1()))
    n = len(items)
    if rand:
        while True:
            yield items[random.randrange(0, n)]
    else:
        i = n-1
        while True:
            i = (i+1)%n
            yield items[i]

def dict_deep_get(dictionary: dict, keys: List[AnyStr]):
    return reduce(lambda d, key: d.get(key) if (type(d) is dict and key in d) else None, keys, dictionary)

def dict_deep_set(dictionary: dict, keys: List[AnyStr], value):
    if len(keys)==0:
        return
    container = reduce(lambda d, key: d.get(key) if (type(d) is dict and key in d) else {}, keys[:-1], dictionary)
    container[keys[-1]] = value

def dict_deep_remove(dictionary: dict, keys: List[AnyStr]):
    if keys is not None:
        if len(keys)==0:
            return
        container = dict_deep_get(dictionary, keys[:-1])
        if container is not None and keys[-1] in container:
            del container[keys[-1]]

def flatMapApply(item:Any, mappers: List[Callable[[Any], Generator[Any, None, None]]], **kwargs) -> Generator[Any, None, None]:
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

def kill_threads_processes(threads: List[Any], ignore_exception:bool=True):
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

def block_join_threads_or_processes(threads: List[Any], 
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
    
def basename_backwards_x3(path: str) -> str:
    return basename_backwards(path, 4)

def basename_backwards_x4(path: str) -> str:
    return basename_backwards(path, 5)

def basename_backwards_x2(path: str) -> str:
    return basename_backwards(path, 3)

def truncate_str_255(txt: str) -> str:
    return txt if txt is None else txt[0:min(254, len(txt))]

def truncate_str_270(txt: str) -> str:
    return txt if txt is None else txt[0:min(269, len(txt))]

def format_duree(duree_sec: int) -> str:
    duree_sec = math.floor(duree_sec)
    d = 0
    h = int(duree_sec//3600)
    if h > 24:
        d = int(h//24)
        h = h%24
    m = int((duree_sec%3600)//60)
    s = duree_sec - (h*3600 + m*60)
    return '{}d {}h {}m {}sec'.format(d, h, m, s)
    
def make_thread_process(use_thread: bool, target, args) -> threading.Thread | multiprocessing.Process:
    if use_thread:
        return threading.Thread(target=target, args=args)
    else:
        p = multiprocessing.Process(target=target, args=args)
        return p

def set_process_affinity(process, cpus_affinities: Set[int], logger: Logger=None, print_log: bool=False, log_prefix: str='', log_level=INFO):
    if isinstance(process, multiprocessing.Process):
        pid = process.pid
        if pid is not None:
            if sys.platform.startswith('win32'):
                set_process_affinity_mask(pid, reduce( lambda acc, x: acc + math.floor(math.pow(2, x)), cpus_affinities, 0))
                if print_log and logger is not None:
                    logger.log(log_level, '{} CPU  N° {} used as process affinity'.format(log_prefix, cpus_affinities))

def get_dir_size_in_mo(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size/1024/1024
#=========================================================== Classes :
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
        

class AbstractConcurrentKeyBagSet(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def add_if_absent(self, bag_key, value) -> bool:
        pass

    @abstractmethod
    def clear(self, bag_key):
        pass

    @abstractmethod
    def clearAll(self):
        pass

    @abstractmethod
    def contains(self, bag_key, value):
        pass
    
    def __contains__(self, bag_key_and_value: Tuple[Any, Any]) -> bool:
        self.contains(bag_key_and_value[0], bag_key_and_value[1])

class ConcurrentKeyBagSet(AbstractConcurrentKeyBagSet):
    def __init__(self, lock: multiprocessing.Lock) -> None:
        self.lock = lock
        self.content = dict()

    def add_if_absent(self, bag_key, value) -> bool:
        self.lock.acquire()
        added=False
        try:
            if bag_key not in self.content:
                self.content[bag_key] = set()
            bag = self.content[bag_key]
            if value not in bag:
                bag.add(value)
                added = True
        except Exception:
            pass
        finally:
            self.lock.release()
        return added

    def clear(self, bag_key):
        self.lock.acquire()
        try:
            if bag_key in self.content:
                self.content[bag_key].clear()
        except Exception:
            pass
        finally:
            self.lock.release()

    def clearAll(self):
        self.lock.acquire()
        try:
            self.content.clear()
        except Exception:
            pass
        finally:
            self.lock.release()

    def contains(self, bag_key, value) -> bool:
        self.lock.acquire()
        try:
            if bag_key in self.content:
                return value in self.content[bag_key]
            return False
        except Exception:
            pass
        finally:
            self.lock.release()