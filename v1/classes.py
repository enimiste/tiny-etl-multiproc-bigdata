from abc import ABC, abstractmethod
import gc
from logging import INFO, WARN, ERROR
from utils import log_msg
 
class AbstractWordSaver(ABC):
    @abstractmethod
    def save_words(self, job_uuid: str, words: list):
        pass

    @abstractmethod
    def close(self):
        pass

class WordsSaverToDB(AbstractWordSaver):
    def __init__(self, job_uuid: str, chunk_size: int, host: str, database: str, user: str, password: str):
        self.job_uuid = job_uuid
        self.connection = None
        self.chunk_size=chunk_size
        self.host=host
        self.user=user
        self.password=password
        self.database = database
        self.query = """INSERT INTO allwordstemp (word, filename, filecount)
                                VALUES(%s,%s,%s)"""
    def _connect(self):
        import mysql.connector

        try:
            if self.connection is None:
                self.connection = mysql.connector.connect(host=self.host, database=self.database, user=self.user, password=self.password)
                log_msg("MySQL connection is opened successfully",  level=INFO)   
            
            if not self.connection.is_connected():
                self.connection.reconnect()
            return self.connection

        except mysql.connector.Error as error:
            log_msg("Failed to connect to database {}".format(str(error.args)), exception=error, level=ERROR)
            raise error

    def save_words(self, job_uuid: str, words: list):
        import mysql.connector

        if self.job_uuid != job_uuid:
            log_msg("Trying to save words from different job. Actual {}, caller {}".format(self.job_uuid, job_uuid),  level=WARN)
            return
        connection = self._connect()
        try:
            data=[[word, file_name, file_words_count] for (word, file_name, file_words_count, infos_dict) in words]
            
            data_len=len(data)
            inserted_data = 0
            log_msg("{0} rows available to be inserted".format(data_len))
            cursor = connection.cursor()
            for ln in range(0, data_len, self.chunk_size):  
                chunk = data[ln:ln+self.chunk_size]
                connection.start_transaction()
                cursor.executemany(self.query, chunk)   
                connection.commit()
                inserted_data+=cursor.rowcount
                log_msg("{} Record inserted successfully".format(cursor.rowcount))
            log_msg("{} Total record inserted successfully".format(inserted_data))
            del(data)
            del(words)
            gc.collect()
            cursor.close()

        except mysql.connector.Error as error:
            log_msg("Failed to insert records {}".format(error), exception=error, level=ERROR)
            if not connection is None and connection.in_transaction():
                try:
                    connection.rollback()
                except Exception as ex:
                    log_msg("Failed to rollback inserted records {}".format(str(ex.args)), exception=ex, level=ERROR)

    def close(self):
        if not self.connection is None and self.connection.is_connected():
            try:
                self.connection.close()
                log_msg("MySQL connection is closed successfully",  level=INFO)
            except Exception as ex:
                log_msg("Error closing MySQL connection", exception=ex , level=ERROR)
            

class WordsSaverToFile(AbstractWordSaver):
    def __init__(self, job_uuid: str, out_dir: str):
        self.out_dir=out_dir
        self.job_uuid = job_uuid
        self.file_hd = None

    def save_words(self, job_uuid: str, words: list):
        import codecs
        import os

        if self.job_uuid != job_uuid:
            log_msg("Trying to save words from different job. Actual {}, caller {}".format(self.job_uuid, job_uuid),  level=WARN)
            return

        if self.file_hd is None:
            file_name = "out_{}.txt".format(job_uuid)
            file_path = os.path.join(self.out_dir, file_name)
            self.file_hd = codecs.open(file_path, 'a', encoding = "utf-8", buffering=1)

        self.file_hd.write('\n'.join([';'.join([w, fl, str(fwc), infos_dict['corpus'], infos_dict['domaine'], infos_dict['periode']])  for (w, fl, fwc, infos_dict) in words]))
        del(words)
        gc.collect()

    def close(self):
        if not self.file_hd is None:
            try:
                self.file_hd.close()
                log_msg("File closed successfully")
            except Exception as ex:
                log_msg("Error closing File handler", exception=ex , level=ERROR)

class WordSaverFactory:
    def __init__(self, config: dict):
        import os
        self.config = {
            'save_to_db': False, 
            'chunk_size': 1000, 
            'db_host': None, 
            'db_name': None, 
            'db_user': None, 
            'db_password': None,
            'out_dir': os.getcwd()
        }
        self.config.update(config)

    def create(self, job_uuid):
        if self.config['save_to_db']:
            return WordsSaverToDB(job_uuid, 
                                    self.config['chunk_size'], 
                                    self.config['db_host'], 
                                    self.config['db_name'], 
                                    self.config['db_user'], 
                                    self.config['db_password'])
        else:
            return WordsSaverToFile(job_uuid, self.config['out_dir'])
