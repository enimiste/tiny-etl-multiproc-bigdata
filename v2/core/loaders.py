from abc import abstractmethod
import io
from logging import INFO, WARN, ERROR, Logger, DEBUG
import multiprocessing
from multiprocessing.sharedctypes import Value
import queue
import threading
from typing import AnyStr, List, Tuple
import uuid

from core.commons import WithLogging
from core.commons import dict_deep_get
from core.commons import rotary_iter
from core.commons import block_join_threads_or_processes
from core.commons import LoggerWrapper
from core.commons import make_thread_process
 
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
                super().log_msg("Item loaded : {}".format(str(self._row_from_data(dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item))), level=self.log_level)

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

    def check_condition(self):
        if callable(self.condition):
            return self.condition()
        else:
            return self.condition

    def loadWithAck(self, job_uuid: str, items: List[dict], ack_counter: Value, last_call: bool) -> None:
        if self.check_condition():
            return self.wrapped_loader.loadWithAck(job_uuid, items, ack_counter, last_call)
        elif self.else_log:
            super().log_msg("Item loaded : {}".format(str(items)))
        ack_counter.value -= 1

    def load(self, job_uuid: str, items: List[dict], last_call: bool) -> None:
        if self.check_condition():
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

class LoadBalanceLoader(AbstractLoader):
    def __init__(self, 
                    logger,
                    loaders: List[Tuple[int, AbstractLoader]],
                    buffer_size: int = 1000,
                    queue_no_block_timeout_sec: int = 0.09,
                    queue_block_timeout_sec: int = 0.1,
                    use_threads_as_loaders_executors: bool = True) -> None:
        super().__init__(logger, None, None)
        self.loaders = loaders
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
                self.loaders_threads.append(make_thread_process(self.use_threads_as_loaders_executors, params["target"], params["args"]))

        for t in self.loaders_threads:
            t.start()
        super().log_msg('{} threads started for loadbalancing'.format(len(self.loaders_threads)), level=INFO)
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


class MySQL_DBLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                input_key_path: List[AnyStr],
                values_path: List[Tuple[str, List[AnyStr], bool]],
                sql_query: str,
                buffer_size: int, 
                host: str, 
                database: str, 
                user: str, 
                password: str):
        super().__init__(logger, input_key_path, values_path)
        self.connection = None
        self.sql_query = sql_query
        self.buffer_size=buffer_size
        self.host=host
        self.user=user
        self.password=password
        self.database = database
        self.buffer = []
        self.calling_thread = Value('i', -1)

    def _row_from_data(self, item: dict)->list:
        row = []
        for (title, key_path, required) in self.values_path:
            val = dict_deep_get(item, key_path)
            if required is not None and required is True and val is None:
                return None
            row.append(val)
        return row

    def _connect(self):
        import mysql.connector

        try:
            if self.connection is None:
                self.connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user, password=self.password)
                super().log_msg("MySQL connection is opened successfully",  level=INFO)   
            
            if not self.connection.is_connected():
                self.connection.reconnect()
            return self.connection

        except mysql.connector.Error as error:
            super().log_msg("Failed to connect to database {}".format(str(error.args)), exception=error, level=ERROR)
            raise error

    def load(self, job_uuid: str, items: List[dict], last_call: bool) -> None:
        id = threading.get_ident()
        if self.calling_thread.value==-1:
            self.calling_thread.value=id
        elif id != self.calling_thread.value:
            raise RuntimeError('Calling the same loader from diffrent threads')

        data = []
        for item in items:
            x = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
            if x is not None:
                d = self._row_from_data(x)
                if d is not None:
                    data.append(d)

        if len(data)>0:
            self.buffer = self.buffer + data

        if last_call or len(self.buffer) > self.buffer_size:
            self.write_buffered_data_to_disk()

    def write_buffered_data_to_disk(self) -> None:
        import mysql.connector

        connection = self._connect()
        try:
            items =  [] + self.buffer
            data_len=len(items)
            inserted_data = 0
            super().log_msg("{0} rows available to be inserted".format(data_len))

            cursor = connection.cursor()
            connection.start_transaction()
            cursor.executemany(self.sql_query, items)   
            connection.commit()

            inserted_data+=cursor.rowcount
            super().log_msg("{} Record inserted successfully".format(cursor.rowcount))
            super().log_msg("{} Total record inserted successfully".format(data_len))
            self.buffer.clear()
            cursor.close()

        except mysql.connector.Error as error:
            super().log_msg("Failed to insert records {}".format(error), exception=error, level=ERROR)
            if not connection is None and connection.in_transaction:
                try:
                    connection.rollback()
                except Exception as ex:
                    super().log_msg("Failed to rollback inserted records {}".format(str(ex.args)), exception=ex, level=ERROR)

    def close(self) -> None:
        try:
            if len(self.buffer) > 0:
                super().log_msg('Flushing buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
                self.write_buffered_data_to_disk()
                self.buffer.clear()
                super().log_msg('Flushed buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
            if self.connection is not None:
                self.connection.close()
            super().log_msg("MySQL connection is closed successfully",  level=INFO)
        except Exception as ex:
            super().log_msg("Error closing MySQL connection", exception=ex , level=ERROR)

    def has_buffered_data(self) -> bool:
        return len(self.buffer)>0
            
class CSV_FileLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                input_key_path: List[AnyStr],
                values_path: List[Tuple[str, List[AnyStr], bool]],
                out_dir: str,
                col_sep: str=";",
                out_file_ext="txt",
                out_file_name_prefix="out_",
                buffer_size: int = 1000
                ):
        super().__init__(logger, input_key_path, values_path)
        self.out_dir=out_dir
        self.file_hd = None
        self.col_sep = col_sep
        self.out_file_ext = out_file_ext
        self.out_file_name_prefix = out_file_name_prefix
        self.calling_thread = Value('i', -1)
        self.buffer_size=buffer_size
        self.buffer = []
        self.uuid = str(uuid.uuid1())

    def _row_from_item(self, item: dict) -> List[AnyStr]:
        row = []
        for (title, key_path, required) in self.values_path:
            val = dict_deep_get(item, key_path)
            if required is not None and required is True and val is None:
                return None
            row.append(str(val))
        return row

    def load(self, job_uuid: str, items: List[dict], last_call: bool):

        id = threading.get_ident()
        if self.calling_thread.value==-1:
            self.calling_thread.value=id
        elif id != self.calling_thread.value:
            raise RuntimeError('Calling the same loader from diffrent threads')
        
        rows = []
        for item in items:
            x = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
            if x is not None:
                row = self._row_from_item(x)
                if row is not None:
                    rows.append(self.col_sep.join(row))

        if len(rows) >0: 
            self.buffer = self.buffer + rows

        if last_call or len(self.buffer) > self.buffer_size:
            self.write_buffered_data_to_disk()

    def _out_filename(self, job_uuid: str) -> str:
        return "{}_{}.{}".format(self.out_file_name_prefix, job_uuid, self.out_file_ext)

    def _open_file(self):
        import codecs
        import os

        if self.file_hd is None:
            file_name = self._out_filename(self.uuid)
            file_path = os.path.join(self.out_dir, file_name)
            self.file_hd = codecs.open(file_path, 'a', encoding = "utf-8")
            super().log_msg("File {} opened using the buffering {}bytes".format(file_path, io.DEFAULT_BUFFER_SIZE))
        return self.file_hd

    def write_buffered_data_to_disk(self):
        rows_nbr = len(self.buffer)  
        if rows_nbr>0:
            fhd = self._open_file()
            fhd.write("\n".join(self.buffer) + "\n")
            super().log_msg("{} total rows written in the file".format(rows_nbr))
            self.buffer.clear()

    def close(self) -> None:
        super().log_msg("Closing loader <>".format(__class__.__name__), level=INFO)
        try:
            if len(self.buffer) > 0:
                super().log_msg('Flushing buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
                self.write_buffered_data_to_disk()
                self.buffer.clear()
                super().log_msg('Flushed buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
            self.file_hd.flush()
            self.file_hd.close()
            super().log_msg("File closed successfully")
        except Exception as ex:
            super().log_msg("Error closing File handler", exception=ex , level=ERROR)

    def has_buffered_data(self) -> bool:
        return len(self.buffer)>0

