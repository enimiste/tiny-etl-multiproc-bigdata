from abc import abstractmethod
from logging import INFO, WARN, ERROR, Logger

from core.commons import WithLogging
from core.commons import dict_deep_get
 
class AbstractLoader(WithLogging):
    def __init__(self, logger: Logger, 
                input_key_path: list[str],
                values_path: list[tuple[str, list[str]]]) -> None:
        super().__init__(logger)
        self.input_key_path = input_key_path
        self.values_path = values_path
        
    @abstractmethod
    def load(self, job_uuid: str, items: list[dict]) -> None:
        pass

    @abstractmethod
    def close(self, job_uuid: str) -> None:
        pass


class NoopLoader(AbstractLoader):
    def __init__(self, logger, 
                input_key_path: list[str],
                values_path: list[tuple[str, list[str]]] = [],
                log: bool = False) -> None:
        super().__init__(logger, input_key_path, values_path)
        self.log = log

    def load(self, job_uuid: str, items: list[dict]) -> None:
        if self.log:
            for item in items:
                super().log_msg("Loading Item : {}".format(str(dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item)))

    def close(self, job_uuid: str) -> None:
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

    def close(self, job_uuid: str) -> None:
        if self._condition():
            self.wrapped_loader.close()


class MySQL_DBLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                input_key_path: list[str],
                values_path: list[tuple[str, list[str]]],
                sql_query: str,
                chunk_size: int, 
                host: str, 
                database: str, 
                user: str, 
                password: str):
        super().__init__(logger, input_key_path, values_path)
        self.connection = None
        self.sql_query = sql_query
        self.chunk_size=chunk_size
        self.host=host
        self.user=user
        self.password=password
        self.database = database

    def _row_from_data(self, item: dict)->list:
        row = []
        for (title, key_path) in self.values_path:
            row.append(dict_deep_get(item, key_path))
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
        import mysql.connector

        connection = self._connect()
        try:
            data = []
            for item in items:
                x = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
                if x is not None:
                    data.append(self._row_from_data(x))

            data_len=len(data)
            inserted_data = 0
            input_data = len(items)
            super().log_msg("{0} rows available to be inserted".format(data_len))
            cursor = connection.cursor()
            for ln in range(0, data_len, self.chunk_size):  
                chunk = data[ln:ln+self.chunk_size]
                connection.start_transaction()
                cursor.executemany(self.sql_query, chunk)   
                connection.commit()
                inserted_data+=cursor.rowcount
                super().log_msg("{} Record inserted successfully".format(cursor.rowcount))
            super().log_msg("{}/input_data={} Total record inserted successfully".format(input_data))
            cursor.close()

        except mysql.connector.Error as error:
            super().log_msg("Failed to insert records {}".format(error), exception=error, level=ERROR)
            if not connection is None and connection.in_transaction():
                try:
                    connection.rollback()
                except Exception as ex:
                    super().log_msg("Failed to rollback inserted records {}".format(str(ex.args)), exception=ex, level=ERROR)

    def close(self) -> None:
        if not self.connection is None and self.connection.is_connected():
            try:
                self.connection.close()
                super().log_msg("MySQL connection is closed successfully",  level=INFO)
            except Exception as ex:
                super().log_msg("Error closing MySQL connection", exception=ex , level=ERROR)
            
class CSV_FileLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                input_key_path: list[str],
                values_path: list[tuple[str, list[str]]],
                out_dir: str,
                col_sep: str=";",
                out_file_ext="txt",
                out_file_name_prefix="out_",
                ):
        super().__init__(logger, input_key_path, values_path)
        self.out_dir=out_dir
        self.file_hd = None
        self.col_sep = col_sep
        self.out_file_ext = out_file_ext
        self.out_file_name_prefix = out_file_name_prefix

    def _row_from_item(self, item: dict) -> list[str]:
        row = []
        for (title, key_path) in self.values_path:
            row.append(str(dict_deep_get(item, key_path)))
        return row

    def load(self, job_uuid: str, items: list[dict]):
        import codecs
        import os

        if self.file_hd is None:
            file_name = self._out_filename(job_uuid)
            file_path = os.path.join(self.out_dir, file_name)
            self.file_hd = codecs.open(file_path, 'a', encoding = "utf-8")
            super().log_msg("File {} opened".format(file_path))
        rows = []
        for item in items:
            x = dict_deep_get(item, self.input_key_path) if self.input_key_path is not None else item
            if x is not None and self._filter(x):
                rows.append(self.col_sep.join(self._row_from_item(x)))

        rows_nbr = len(rows)  
        if rows_nbr>0:
            self.file_hd.write("\n".join(rows) + "\n")
            super().log_msg("{}/input_data={} total rows written in the file".format(rows_nbr, len(items)))

    def _out_filename(self, job_uuid: str) -> str:
        return "{}_{}.{}".format(self.out_file_name_prefix, job_uuid, self.out_file_ext)

    def _filter(self, item: dict) -> bool:
        return item is not None

    def close(self) -> None:
        if not self.file_hd is None:
            try:
                self.file_hd.flush()
                self.file_hd.close()
                super().log_msg("File closed successfully")
            except Exception as ex:
                super().log_msg("Error closing File handler", exception=ex , level=ERROR)
