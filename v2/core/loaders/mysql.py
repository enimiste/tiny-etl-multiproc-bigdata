from logging import INFO, WARN, ERROR, Logger, DEBUG
from multiprocessing.sharedctypes import Value
import threading
from typing import AnyStr, List, Set, Tuple

from core.commons import dict_deep_get
from core.loaders.commons import AbstractLoader


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
                con = mysql.connector.connect(host=self.host, database=self.database, user=self.user, password=self.password)
                self.connection = (con, con.cursor())
                super().log_msg("MySQL connection is opened successfully  <{}>".format(self.connection[0].__class__.__name__),  level=INFO)   
            
            if not self.connection[0].is_connected():
                self.connection[0].reconnect()
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

        (connection, cursor) = self._connect()
        reconnection_retries = 0
        while True:
            try:
                items =  [] + self.buffer
                data_len=len(items)
                inserted_data = 0
                super().log_msg("{0} rows available to be inserted".format(data_len))

                connection.start_transaction()
                cursor.executemany(self.sql_query, items)   
                connection.commit()

                inserted_data+=cursor.rowcount
                super().log_msg("{} Record inserted successfully".format(cursor.rowcount))
                super().log_msg("{} Total record inserted successfully".format(data_len))
                self.buffer.clear()
            except mysql.connector.OperationalError as error:
                connection = self._connect()
                if reconnection_retries<5:
                    reconnection_retries += 1
                    super().log_msg("Reconnection {}/5".format(reconnection_retries), level=INFO)
                    continue
                else:
                    super().log_msg("Reconnection max retries reached (=5)".format(reconnection_retries), level=INFO)
            except mysql.connector.DataError as error:
                super().log_msg("Failed to insert records. Error={}".format(error), exception=error, level=ERROR)
                if connection is not None and connection.in_transaction:
                    try:
                        connection.rollback()
                    except Exception as ex:
                        super().log_msg("Failed to rollback inserted records {}".format(ex.args), exception=ex, level=ERROR)
            except mysql.connector.Error as error:
                super().log_msg("Failed to insert records. Error={}".format(error), exception=error, level=ERROR)
            
            return

    def close(self) -> None:
        try:
            if len(self.buffer) > 0:
                super().log_msg('Flushing buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
                self.write_buffered_data_to_disk()
                self.buffer.clear()
                super().log_msg('Flushed buffered data in <{}>'.format(str(self.__class__.__name__)), level=INFO)
            if self.connection is not None:
                (conn, cursor) = self.connection
                cursor.close()
                conn.close()
            super().log_msg("MySQL connection is closed successfully",  level=INFO)
        except Exception as ex:
            super().log_msg("Error closing MySQL connection", exception=ex , level=ERROR)

    def has_buffered_data(self) -> bool:
        return len(self.buffer)>0
