import traceback
from concurrent_log_handler import ConcurrentRotatingFileHandler
import logging
from logging import INFO, ERROR, WARN, Logger
import time
from datetime import date
import os
import sys
from core.extractors import FilesListExtractor
from core.loaders import CSVFileLoader
from core.pipline import ThreadedPipeline
from core.transformers import FilePathToBasenameTransformer, FileToLinesTransformer, TextWordTokenizerTransformer
from loaders import WordsCSVFileLoader
from transformers import ArabicRemoveDiacFromWordTransformer, ArabicTextWordsTokenizerTransformer

LOGGING_FORMAT = '%(levelname)s : %(asctime)s - %(processName)s (%(threadName)s) : %(message)s'
console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
console_handler.setLevel(logging.INFO)
logging.basicConfig(handlers=[ConcurrentRotatingFileHandler(mode="a",
                                                                    filename=os.path.abspath(f'logs/log-{date.today()}.log'),
                                                                    maxBytes=50*1024*1024, backupCount=100), console_handler], 
                                                level=logging.DEBUG,
                                                encoding='utf-8',
                                                format=LOGGING_FORMAT)
LOGGER = logging.getLogger("my-logger")

                          
if __name__=="__main__":

    in_dir='../bdall_test_data'
    out_dir = 'out_dir'
    save_to_db=True
    db_host='localhost'
    db_name='arabic_lang'
    db_user='root'
    db_password='root'
    chunk_size=1000
    show_summary=False

    start_exec_time = time.perf_counter()
    words_saver = None
    config = {
        'save_to_db': save_to_db, 
        'chunk_size': chunk_size, 
        'db_host': db_host, 
        'db_name': db_name, 
        'db_user': db_user, 
        'db_password': db_password,
        'out_dir': out_dir
    }
    LOGGER.log(INFO, "Script started : {}".format(in_dir))
    try:
        pipeline = ThreadedPipeline(LOGGER, 
                            extractor=FilesListExtractor(LOGGER, in_dir, ".txt"),
                            transformers=[
                                    FileToLinesTransformer(LOGGER, ".txt"), 
                                    TextWordTokenizerTransformer(LOGGER, "\\s+"),
                                    FilePathToBasenameTransformer(LOGGER)
                                    ],
                            loaders=[WordsCSVFileLoader(LOGGER, os.path.abspath(out_dir))])
        pipeline.start()
        pipeline.join()
    except Exception as ex:
        LOGGER.log(ERROR, "Trace : {}".format(str(traceback.format_exception(ex))))
    finally:
        end_exec_time=time.perf_counter()
        LOGGER.info('Script executed in {} sec'.format(str(round(end_exec_time-start_exec_time, 3))))
    