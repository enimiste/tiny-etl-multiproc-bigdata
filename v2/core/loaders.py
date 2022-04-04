from abc import abstractmethod
import io
from logging import INFO, WARN, ERROR, Logger
from multiprocessing.sharedctypes import Value
import threading

from core.commons import WithLogging
from core.commons import dict_deep_get
 
class AbstractLoader(WithLogging):
    def __init__(self, logger: Logger, 
                input_key_path: list[str],
                values_path: list[tuple[str, list[str]], bool]) -> None:
        super().__init__(logger)
        self.input_key_path = input_key_path
        self.values_path = values_path
        
    @abstractmethod
    def load(self, job_uuid: str, items: list[dict]) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class NoopLoader(AbstractLoader):
    def __init__(self, logger, 
                input_key_path: list[str],
                values_path: list[tuple[str, list[str], bool]] = [],
                log: bool = False) -> None:
        super().__init__(logger, input_key_path, values_path)
        self.log = log

    def load(self, job_uuid: str, items: list[dict]) -> None:
        if self.log:
            for item in items:
                super().log_msg("Loading Item : {}".format(str(dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item)))

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

    def load(self, job_uuid: str, items: list[dict]) -> None:
        if self.check_condition():
            return self.wrapped_loader.load(job_uuid, items)
        elif self.else_log:
            super().log_msg("Loading : {}".format(str(items)))

    def close(self) -> None:
        if self.check_condition():
            self.wrapped_loader.close()


class MySQL_DBLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                input_key_path: list[str],
                values_path: list[tuple[str, list[str], bool]],
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

    def load(self, job_uuid: str, items: list[dict]) -> None:
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

        if len(self.buffer) > self.buffer_size:
            self.write_buffered_data_to_disk()

    def write_buffered_data_to_disk(self) -> None:
        import mysql.connector

        connection = self._connect()
        try:
            data_len=len(self.buffer)
            inserted_data = 0
            super().log_msg("{0} rows available to be inserted".format(data_len))

            cursor = connection.cursor()
            connection.start_transaction()
            cursor.executemany(self.sql_query, self.buffer)   
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
        if not self.connection is None and self.connection.is_connected():
            try:
                if len(self.buffer) > 0:
                    self.write_buffered_data_to_disk()
                    self.buffer.clear()
                self.connection.close()
                super().log_msg("MySQL connection is closed successfully",  level=INFO)
            except Exception as ex:
                super().log_msg("Error closing MySQL connection", exception=ex , level=ERROR)
            
class CSV_FileLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                input_key_path: list[str],
                values_path: list[tuple[str, list[str], bool]],
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

    def _row_from_item(self, item: dict) -> list[str]:
        row = []
        for (title, key_path, required) in self.values_path:
            val = dict_deep_get(item, key_path)
            if required is not None and required is True and val is None:
                return None
            row.append(str(val))
        return row

    def load(self, job_uuid: str, items: list[dict]):
        import codecs
        import os

        id = threading.get_ident()
        if self.calling_thread.value==-1:
            self.calling_thread.value=id
        elif id != self.calling_thread.value:
            raise RuntimeError('Calling the same loader from diffrent threads')

        if self.file_hd is None:
            file_name = self._out_filename(job_uuid)
            file_path = os.path.join(self.out_dir, file_name)
            self.file_hd = codecs.open(file_path, 'a', encoding = "utf-8")
            super().log_msg("File {} opened using the buffering {}bytes".format(file_path, io.DEFAULT_BUFFER_SIZE))
        
        rows = []
        for item in items:
            x = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
            if x is not None:
                row = self._row_from_item(x)
                if row is not None:
                    rows.append(self.col_sep.join(row))

        if len(rows) >0: 
            self.buffer = self.buffer + rows

        if len(self.buffer) > self.buffer_size:
            self.write_buffered_data_to_disk()

    def _out_filename(self, job_uuid: str) -> str:
        return "{}_{}.{}".format(self.out_file_name_prefix, job_uuid, self.out_file_ext)

    def write_buffered_data_to_disk(self):
        rows_nbr = len(self.buffer)  
        if rows_nbr>0:
            self.file_hd.write("\n".join(self.buffer) + "\n")
            super().log_msg("{} total rows written in the file".format(rows_nbr))
        self.buffer.clear()

    def close(self) -> None:
        super().log_msg("Closing loader <>".format(__class__.__name__), level=INFO)
        if not self.file_hd is None:
            try:
                if len(self.buffer) > 0:
                    self.write_buffered_data_to_disk()
                    self.buffer.clear()
                self.file_hd.flush()
                self.file_hd.close()
                super().log_msg("File closed successfully")
            except Exception as ex:
                super().log_msg("Error closing File handler", exception=ex , level=ERROR)

