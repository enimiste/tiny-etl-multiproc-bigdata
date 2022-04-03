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
from core.transformers import ReduceTransformer
from core.transformers import FileTextReaderTransformer
from core.transformers import AddStaticValuesTransformer

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
    #in_dir='../bdall_test_data/corpusB/base1'
    out_dir = 'out_dir'
    save_to_db=False
    db_host='localhost'
    db_name='arabic_lang'
    db_user='root'
    db_password='root'
    buffer_size=1000
    show_summary=False

    start_exec_time = time.perf_counter()
    words_saver = None
    config = {
        'save_to_db': save_to_db, 
        'buffer_size': buffer_size, 
        'db_host': db_host, 
        'db_name': db_name, 
        'db_user': db_user, 
        'db_password': db_password,
        'out_dir': out_dir
    }
    LOGGER.log(INFO, "Script started : {}".format(in_dir))
    try:
        pipeline = ThreadedPipeline(LOGGER, 
                            max_transformation_pipelines=5,
                            use_threads_as_transformation_pipelines=True,
                            trans_in_queue_max_size=1_000,
                            extractor=FilesListExtractor(LOGGER, intput_dir=in_dir, pattern=".txt", output_key='_'),
                            transformers=[
                                    #NoopTransformer(LOGGER, log=True, log_level=INFO, log_prefix='X'),
                                    ItemUpdaterCallbackTransformer(LOGGER, input_key_path=['_'], callback=os.path.abspath),
                                    # AddStaticValuesTransformer(LOGGER,
                                    #                             static_values=[(['words_count'], 0)],
                                    #                             copy_values_key_paths=[('file_path', ['_'])],
                                    #                             remove_key_paths = [['_']]),
                                    ReduceTransformer(  LOGGER,
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
                                                        reducer=ReduceTransformer.count),
                                    FileToTextLinesTransformer(LOGGER, pattern=".txt", input_key_path=['file_path'], output_key='_',
                                                            copy_values_key_paths=[('file_path', ['file_path']), ('words_count', ['words_count'])]), 
                                    TextWordTokenizerTransformer(LOGGER, 
                                                                   pattern="\\s+", 
                                                                   input_key_path=['_', 'line'], 
                                                                   output_key='_', 
                                                                   copy_values_key_paths=[('file_path', ['file_path']), ('words_count', ['words_count'])]),
                                    ItemUpdaterCallbackTransformer(LOGGER, input_key_path=['file_path'], callback=os.path.basename)
                                    ],
                            loaders=[
                                    ConditionalLoader(  LOGGER, 
                                                        not config['save_to_db'],
                                                        # False,
                                                        CSV_FileLoader( LOGGER,
                                                                        input_key_path=None,
                                                                        values_path=[('word', ['_', 'word']), 
                                                                                        ('file', ['file_path']),
                                                                                        ('words_count', ['words_count'])],
                                                                        out_dir=os.path.abspath(out_dir),
                                                                        out_file_ext='txt',
                                                                        buffer_size=config['buffer_size'])),
                                    # ConditionalLoader(  LOGGER, 
                                    #                     not config['save_to_db'],
                                    #                     CSV_FileLoader( LOGGER,
                                    #                                     input_key_path=None,
                                    #                                     values_path=[('word', ['_', 'word']), 
                                    #                                                     ('file', ['file_path']),
                                    #                                                     ('words_count', ['words_count'])],
                                    #                                     out_dir=os.path.abspath(out_dir),
                                    #                                     out_file_ext='csv')),
                                    # ConditionalLoader( LOGGER, 
                                    #                    config['save_to_db'],
                                    #                    MySQL_DBLoader( LOGGER, 
                                    #                                    input_key_path=['_'], 
                                    #                                    values_path=[('word', ['_', 'word']), 
                                    #                                                  ('file', ['file_path']),
                                    #                                                  ('words_count', ['words_count'])],
                                    #                                    sql_query= """INSERT INTO allwordstemp (word, filename, filecount)
                                    #                                                    VALUES(%s,%s,0)""",
                                    #                                    buffer_size=config['buffer_size'],
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
    