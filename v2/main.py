import traceback
from concurrent_log_handler import ConcurrentRotatingFileHandler
import logging
from logging import INFO, ERROR, WARN, Logger
import time
from datetime import date
import os
import sys
from core.extractors import FilesListExtractor
from core.loaders import CSV_FileLoader
from core.pipline import ThreadedPipeline
from core.transformers import  FileToLinesTransformer, TextWordTokenizerTransformer
from transformers import ArabicRemoveDiacFromWordTransformer, ArabicTextWordsTokenizerTransformer
from core.transformers import ItemUpdaterCallbackTransformer
from core.loaders import ConditionalLoader, MySQL_DBLoader
from core.transformers import NoopTransformer

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

    in_dir='../bdall_test_data/corpusB/base1'
    out_dir = 'out_dir'
    save_to_db=False
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
                            max_transformation_pipelines=1,
                            use_threads_as_transformation_pipelines=True,
                            extractor=FilesListExtractor(LOGGER, intput_dir=in_dir, pattern=".txt", output_key='_'),
                            transformers=[
                                    #NoopTransformer(LOGGER),
                                    FileToLinesTransformer(LOGGER, pattern=".txt", input_key_path=['_'], output_key='_'), 
                                    TextWordTokenizerTransformer(LOGGER, 
                                                                   pattern="\\s+", 
                                                                   input_key_path=['_', 'line'], 
                                                                   output_key='_', 
                                                                   copy_values_key_paths=[('file_path', ['_', 'file_path'])]),
                                    ItemUpdaterCallbackTransformer(LOGGER, input_key_path=['file_path'], callback=os.path.basename)
                                    ],
                            loaders=[
                                    ConditionalLoader( LOGGER, 
                                                        not config['save_to_db'],
                                                        CSV_FileLoader( LOGGER,
                                                                        input_key_path=None,
                                                                        values_path=[('word', ['_', 'word']), 
                                                                                     ('file', ['file_path'])],
                                                                        out_dir=os.path.abspath(out_dir))),

                                    # ConditionalLoader( LOGGER, 
                                    #                    config['save_to_db'],
                                    #                    MySQL_DBLoader( LOGGER, 
                                    #                                    input_key_path=['_'], 
                                    #                                    sql_query= """INSERT INTO allwordstemp (word, filename, filecount)
                                    #                                                    VALUES(%s,%s,%s)""",
                                    #                                    chunk_size=config['chunk_size'],
                                    #                                    host=config['db_host'],
                                    #                                    database=config['db_name'],
                                    #                                    user=config['db_user'],
                                    #                                    password=config['db_password']))
                                                            ])
        pipeline.start()
        pipeline.join()
    except Exception as ex:
        LOGGER.log(ERROR, "Trace : {}".format(str(traceback.format_exception(ex))))
    finally:
        end_exec_time=time.perf_counter()
        LOGGER.info('Script executed in {} sec'.format(str(round(end_exec_time-start_exec_time, 3))))
    