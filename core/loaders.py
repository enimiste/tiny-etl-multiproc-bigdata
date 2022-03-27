from abc import abstractmethod
from logging import INFO, WARN, ERROR, Logger

from core.commons import WithLogging
 
class AbstractLoader(WithLogging):
    def __init__(self, logger: Logger) -> None:
        super().__init__(logger)
        
    @abstractmethod
    def load(self, job_uuid: str, items: list[dict]) -> None:
        pass

    @abstractmethod
    def close(self, job_uuid: str) -> None:
        pass

class MySQLDBLoader(AbstractLoader):
    def __init__(self, 
                logger: Logger, 
                chunk_size: int, 
                host: str, 
                database: str, 
                user: str, 
                password: str):
        super().__init__(logger)
        self.connection = None
        self.chunk_size=chunk_size
        self.host=host
        self.user=user
        self.password=password
        self.database = database
    

    @abstractmethod
    def _query(self) -> str:
        pass

    @abstractmethod
    def _row_from_data(self, item:dict)->list:
        pass

    def _connect(self):
        import mysql.connector

        try:
            if self.connection is None:
                self.connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user, password=self.password)
                self.log_msg("MySQL connection is opened successfully",  level=INFO)   
            
            if not self.connection.is_connected():
                self.connection.reconnect()
            return self.connection

        except mysql.connector.Error as error:
            self.log_msg("Failed to connect to database {}".format(str(error.args)), exception=error, level=ERROR)
            raise error

    def load(self, job_uuid: str, items: list[dict]) -> None:
        import mysql.connector

        connection = self._connect()
        try:
            data=[self._row_from_data(item) for item in items]
            
            data_len=len(data)
            inserted_data = 0
            self.log_msg("{0} rows available to be inserted".format(data_len))
            cursor = connection.cursor()
            for ln in range(0, data_len, self.chunk_size):  
                chunk = data[ln:ln+self.chunk_size]
                connection.start_transaction()
                cursor.executemany(self._query(), chunk)   
                connection.commit()
                inserted_data+=cursor.rowcount
                self.log_msg("{} Record inserted successfully".format(cursor.rowcount))
            self.log_msg("{} Total record inserted successfully".format(inserted_data))
            cursor.close()

        except mysql.connector.Error as error:
            self.log_msg("Failed to insert records {}".format(error), exception=error, level=ERROR)
            if not connection is None and connection.in_transaction():
                try:
                    connection.rollback()
                except Exception as ex:
                    self.log_msg("Failed to rollback inserted records {}".format(str(ex.args)), exception=ex, level=ERROR)

    def close(self) -> None:
        if not self.connection is None and self.connection.is_connected():
            try:
                self.connection.close()
                self.log_msg("MySQL connection is closed successfully",  level=INFO)
            except Exception as ex:
                self.log_msg("Error closing MySQL connection", exception=ex , level=ERROR)
            
class CSVFileLoader(AbstractLoader):
    def __init__(self, logger: Logger, out_dir: str):
        super().__init__(logger)
        self.out_dir=out_dir
        self.file_hd = None

    @abstractmethod
    def _row_from_item(self, item: dict) -> list:
        pass

    def load(self, job_uuid: str, items: list[dict]):
        import codecs
        import os

        if self.file_hd is None:
            file_name = self._out_filename(job_uuid)
            file_path = os.path.join(self.out_dir, file_name)
            self.file_hd = codecs.open(file_path, 'a', encoding = "utf-8", buffering=1)

        rows = [self._col_sep().join(self._row_from_item(d))  for d in items if self._filter(d)]
        if len(rows)>0:
            self.file_hd.write('\n'.join())

    def _out_filename(self, job_uuid: str) -> str:
        return "out_{}.txt".format(job_uuid)

    def _col_sep(self) -> str:
        return ';'

    def _filter(self, item: dict) -> bool:
        return True

    def close(self) -> None:
        if not self.file_hd is None:
            try:
                self.file_hd.close()
                self.log_msg("File closed successfully")
            except Exception as ex:
                self.log_msg("Error closing File handler", exception=ex , level=ERROR)

