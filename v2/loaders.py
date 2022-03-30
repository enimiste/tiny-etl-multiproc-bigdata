
from logging import Logger
import os
from core.commons import WithLogging
from core.loaders import AbstractLoader, CSVFileLoader, MySQLDBLoader

class WordsDBLoader(MySQLDBLoader):
    def __init__(self, logger: Logger, job_uuid: str, chunk_size: int, host: str, database: str, user: str, password: str):
        super().__init__(logger, job_uuid, chunk_size, host, database, user, password)
        
    def _query(self) -> str:
        return """INSERT INTO allwordstemp (word, filename, filecount)
                                VALUES(%s,%s,%s)"""

    def _row_from_data(self, item: dict) -> list:
        (word, file_name, file_words_count, infos_dict) = item
        return [word, file_name, file_words_count]

class WordsCSVFileLoader(CSVFileLoader):
    def __init__(self, logger: Logger, out_dir: str):
        super().__init__(logger, out_dir)

    def _row_from_item(self, item: dict) -> list[str]:
        return [item['_'], item['file_path']]

    def _filter(self, item: dict) -> bool:
        return item is not None and '_' in item and 'file_path' in item

class WordSaverFactory(WithLogging):
    def __init__(self, logger: Logger, config: dict) -> None:
        super().__init__(logger)
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

    def create(self) -> AbstractLoader:
        if self.config['save_to_db']:
            return WordsDBLoader(self.logger,
                                    self.config['chunk_size'], 
                                    self.config['db_host'], 
                                    self.config['db_name'], 
                                    self.config['db_user'], 
                                    self.config['db_password'])
        else:
            return WordsCSVFileLoader(self.logger, self.config['out_dir'])
          