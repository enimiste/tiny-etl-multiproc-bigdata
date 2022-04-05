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
from core.transformers import  FileToTextLinesTransformer, TextWordTokenizerTransformer
from transformers import ArabicRemoveDiacFromWordTransformer, ArabicTextWordsTokenizerTransformer
from core.transformers import ItemUpdaterCallbackTransformer
from core.loaders import ConditionalLoader, MySQL_DBLoader
from core.transformers import NoopTransformer
from core.transformers import ReduceItemTransformer
from core.transformers import FileTextReaderTransformer
from core.transformers import AddStaticValuesTransformer
from core.loaders import LoadBalanceLoader
from core.loaders import NoopLoader
from core.commons import basename_backwards

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
                          

def basename_backwards_x3(path: str) -> str:
    return basename_backwards(path, 3)

def basename_backwards_x2(path: str) -> str:
    return basename_backwards(path, 2)

if __name__=="__main__":
    config = {
        #'in_dir': '../bdall_test_data/one_book',
        'in_dir': '../bdall_test_data/__generated/books_0',
        'out_dir': 'out_dir',
        'save_to_db': True, 
        'buffer_size': 10_000, 
        'db_host': 'localhost', 
        'db_name': 'words', 
        'db_user': 'root', 
        'db_password': 'root',
        'parallel_loader_count': 20,
        'max_transformation_pipelines': 100,
        'use_threads_as_transformation_pipelines':False,
        'load_balancer_buffer_size': 1000
    }

    start_exec_time = time.perf_counter()
    words_saver = None
    
    mysql_db_loader_config =  {
            'input_key_path': None, 
            'values_path': [('word', ['_', 'word'], True), 
                            ('file', ['file_path'], True),
                            ('words_count', ['words_count'], True)],
        #    sql_query= """INSERT INTO allwordstemp (word, filename, filecount)
        #                    VALUES(%s,%s,0)""",
            'sql_query': """INSERT INTO words (word, file_path, file_words_count)
                            VALUES(%s,%s,%s)""",
            'buffer_size': config['buffer_size'],
            'host': config['db_host'],
            'database': config['db_name'],
            'user': config['db_user'],
            'password': config['db_password']}
            
    LOGGER.log(INFO, "Script started : {}".format(config['in_dir']))
    try:
        pipeline = ThreadedPipeline(LOGGER, 
                            max_transformation_pipelines=config['max_transformation_pipelines'],
                            use_threads_as_transformation_pipelines=config['use_threads_as_transformation_pipelines'],
                            trans_in_queue_max_size=config['buffer_size'],
                            extractor=FilesListExtractor(LOGGER, intput_dir=config['in_dir'], pattern=".txt", output_key='_'),
                            transformers=[
                                    #NoopTransformer(LOGGER, log=True, log_level=INFO, log_prefix='X'),
                                    ItemUpdaterCallbackTransformer(LOGGER, input_key_path=['_'], callback=os.path.abspath),
                                    # AddStaticValuesTransformer(LOGGER,
                                    #                             static_values=[(['words_count'], 0)],
                                    #                             copy_values_key_paths=[('file_path', ['_'])],
                                    #                             remove_key_paths = [['_']]),
                                    ReduceItemTransformer(  LOGGER,
                                                        input_key_path=['_'], 
                                                        input_value_type=(str),
                                                        output_key='words_count', 
                                                        copy_values_key_paths=[('file_path', ['_'])],
                                                        transformers=[
                                                                        FileTextReaderTransformer(LOGGER, 
                                                                                                pattern=".txt", 
                                                                                                input_key_path=None, 
                                                                                                output_key=None),
                                                                        TextWordTokenizerTransformer(   LOGGER, 
                                                                                                    pattern="\\s+", 
                                                                                                    input_key_path=['_', 'content'], 
                                                                                                    output_key=None)
                                                                    ],
                                                        initial_value=0,
                                                        reducer=ReduceItemTransformer.count),
                                    FileToTextLinesTransformer(LOGGER, pattern=".txt", input_key_path=['file_path'], output_key='_',
                                                            copy_values_key_paths=[('file_path', ['file_path']), ('words_count', ['words_count'])]), 
                                    TextWordTokenizerTransformer(LOGGER, 
                                                                   pattern="\\s+", 
                                                                   input_key_path=['_', 'line'], 
                                                                   output_key='_', 
                                                                   copy_values_key_paths=[('file_path', ['file_path']), ('words_count', ['words_count'])]),
                                    ItemUpdaterCallbackTransformer(LOGGER, input_key_path=['file_path'], callback=basename_backwards_x3),
                                    # ItemUpdaterCallbackTransformer(LOGGER, input_key_path=['file_path'], callback=os.path.basename),
                                    ],
                            loaders=[
                                    # ConditionalLoader(  LOGGER, 
                                    #                     not config['save_to_db'],
                                    #                     CSV_FileLoader( LOGGER,
                                    #                                     input_key_path=None,
                                    #                                     values_path=[('word', ['_', 'word'], True), 
                                    #                                                     ('file', ['file_path'], True),
                                    #                                                     ('words_count', ['words_count'], True)],
                                    #                                     out_dir=os.path.abspath(config['out_dir']),
                                    #                                     out_file_ext='txt',
                                    #                                     buffer_size=config['buffer_size'])),
                                    # ConditionalLoader(  LOGGER, 
                                    #                     config['save_to_db'],
                                    #                     #  False,
                                    #                     MySQL_DBLoader( LOGGER, **mysql_db_loader_config)
                                    # ),
                                    ConditionalLoader(  LOGGER, 
                                                        config['save_to_db'],
                                                        # False,
                                                        LoadBalanceLoader(LOGGER, 
                                                                        queue_no_block_timeout_sec = 0.09,
                                                                        buffer_size=config['load_balancer_buffer_size'],
                                                                        loaders= [(
                                                                                    config['buffer_size']*10, 
                                                                                    MySQL_DBLoader( LOGGER, **mysql_db_loader_config)) 
                                                                                    for _ in range(0, max(1, config['parallel_loader_count']))]
                                                                        # loaders= [(
                                                                        #             config['buffer_size']*10, 
                                                                        #             NoopLoader( LOGGER, 
                                                                        #                         input_key_path=None,
                                                                        #                         values_path=[('word', ['_', 'word'], None), 
                                                                        #                                     ('file', ['file_path'], None),
                                                                        #                                     ('words_count', ['words_count'], None)],
                                                                        #                         log=True,
                                                                        #                         log_level=INFO)) 
                                                                        #                         for i in range(0, max(1, config['parallel_loader_count']))]
                                                                        ))
                                    ])
        pipeline.start()
        pipeline.join()
    except Exception as ex:
        LOGGER.log(ERROR, "Trace : {}".format(str(traceback.format_exception(ex))))
    finally:
        end_exec_time=time.perf_counter()
        LOGGER.info('Script executed in {} sec'.format(str(round(end_exec_time-start_exec_time, 3))))
    