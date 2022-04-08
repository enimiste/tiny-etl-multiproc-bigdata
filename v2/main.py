import json
import math
import traceback
from concurrent_log_handler import ConcurrentRotatingFileHandler
import logging
from logging import INFO, ERROR, WARN, Logger
import time
from datetime import date
import os
import sys

import psutil
from core.extractors import FilesListExtractor
from core.loaders import CSV_FileLoader
from core.pipline import ThreadedPipeline
from core.transformers import  FileToTextLinesTransformer, TextWordTokenizerTransformer
from custom_transformers import ArabicRemoveDiacFromWordTransformer, ArabicTextWordsTokenizerTransformer
from core.transformers import ItemAttributeTransformer
from core.loaders import ConditionalLoader, MySQL_DBLoader
from core.transformers import NoopTransformer
from core.transformers import ReduceItemTransformer
from core.transformers import FileTextReaderTransformer
from core.transformers import AddStaticValuesTransformer
from core.loaders import LoadBalanceLoader
from core.loaders import NoopLoader
from core.commons import basename_backwards
from core.commons import block_join_threads_or_processes
from core.commons import get_dir_size_in_mo
from core.transformers import UniqueFilterTransformer

LOGGING_FORMAT = '%(name)s %(levelname)s : %(asctime)s - %(processName)s (%(threadName)s) : %(message)s'
console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
console_handler.setLevel(logging.INFO)
logging.basicConfig(handlers=[ConcurrentRotatingFileHandler(mode="a",
                                                                    filename=os.path.abspath(f'logs/log-{date.today()}.log'),
                                                                    maxBytes=50*1024*1024, backupCount=100), console_handler], 
                                                level=logging.DEBUG,
                                                encoding='utf-8',
                                                format=LOGGING_FORMAT)
LOGGER = logging.getLogger("Global")
                          

def basename_backwards_x3(path: str) -> str:
    return basename_backwards(path, 3)

def basename_backwards_x2(path: str) -> str:
    return basename_backwards(path, 2)

if __name__=="__main__":
    #0.00050067901 sec/ko
    config = {
        # 'in_dir': '../bdall_test_data/__generated',
        'in_dir': '../bdall_test_data/small_data',
        'out_dir': 'out_dir',
        'save_to_db': True, 
        'buffer_size': 10_000,#10_000 optimal
        'db_host': 'localhost', 
        'db_name': 'words', 
        'db_user': 'root', 
        'db_password': 'root',
        'max_transformation_pipelines': 2,#2 optimal
        'use_threads_as_transformation_pipelines': False,#False optimal
        'use_threads_as_loaders_executors': False,#False optimal
        'use_threads_as_extractors_executors': False,#False optimal
        'load_balancer_parallel_loader_count': 4,#4 optimal
        'use_threads_as_load_balancer_loaders_executors': True,#True optimal
        'load_balancer_buffer_size': 1_000#1_000 optimal
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
    
    LOGGER.log(INFO, 'Config : {}'.format(json.dumps(config, indent=4)))
    LOGGER.log(INFO, 'MySQL Loader Config : {}'.format(json.dumps(mysql_db_loader_config, indent=4)))

    # Check RAM availability
    in_dir_size_mo = round(get_dir_size_in_mo(config['in_dir']), 3)
    dirs = os.listdir(config['in_dir'])
    nbr_dirs = len(dirs)
    nbr_processes_per_pip=0
    if not config['use_threads_as_extractors_executors'] :
        nbr_processes_per_pip+=1

    if not config['use_threads_as_loaders_executors'] :
        nbr_processes_per_pip+=1

    if not config['use_threads_as_transformation_pipelines'] :
        nbr_processes_per_pip+=config['max_transformation_pipelines']

    if not config['use_threads_as_load_balancer_loaders_executors'] :
        nbr_processes_per_pip+=config['load_balancer_parallel_loader_count']
    
    cpus_count = psutil.cpu_count()
    exec_time_sec = (0.00050067901 * cpus_count/8) * in_dir_size_mo * 1024 #0.00050067901 sec/ko
    nbr_processes = nbr_dirs * nbr_processes_per_pip
    ram_per_process_mo = 100
    ram_mo = math.floor(psutil.virtual_memory()[1]/(1024*1024))
    ram_reserv_mo = 1024
    ram_secur_mo = max(0, ram_mo - ram_reserv_mo)
    estim_processes_mo = nbr_processes*ram_per_process_mo #80Mo by process
    nbr_dirs_secur = max(1, math.ceil((ram_secur_mo/ram_per_process_mo)/math.floor(nbr_processes_per_pip * 1.6))) #1.6 majoration factor
    LOGGER.log(INFO, """
                        IN_DIR size                = {}Mo ({}Go),
                        Execution time             = {} sec ({} min = {} hours),
                        CPU                       ~= {},
                        RAM free                   = {}Mo, 
                        _____________________________________________________________________
                        Nbr processes              = {}, 
                        RAM available              = {}Mo (RAM free - {}Mo), 
                        Estimated RAM              = {}Mo ({}Mo each one) (for all processes), 
                        Recommended root folders   = {} folders (in_dir root folders count),
                        Folders in in_dir          = {} folders""".format(in_dir_size_mo,
                                                                        math.ceil(in_dir_size_mo/1024),
                                                                        math.ceil(exec_time_sec),
                                                                        math.ceil(exec_time_sec/60),
                                                                        math.ceil(exec_time_sec/3600),
                                                                        cpus_count,
                                                                        ram_mo, 
                                                                        nbr_processes, 
                                                                        ram_secur_mo, 
                                                                        ram_reserv_mo, 
                                                                        estim_processes_mo, 
                                                                        ram_per_process_mo,
                                                                        nbr_dirs_secur,
                                                                        nbr_dirs))
    if ram_secur_mo<estim_processes_mo:
        LOGGER.log(INFO, 'RAM not enough for running the {} processes ({}Mo each). You should structure the {} folder to have at most {} root folders, {} found'.format(
            nbr_processes, ram_per_process_mo, config['in_dir'], nbr_dirs_secur, nbr_dirs
        ))
        exit()
    if '-s' not in sys.argv:
        LOGGER.log(INFO, 'To run the script add the -s option to the command')
        exit()
        
    # Start program
    LOGGER.log(INFO, "Script started")
    pipelines = []
    try:
        
        for (idx, dir) in enumerate(dirs):
            in_dir = os.path.join(config['in_dir'], dir)
            _LOGGER=logging.getLogger("Pipeline " + str(idx+1))

            pipelines.append(ThreadedPipeline(_LOGGER, 
                                use_threads_as_extractors_executors=config['use_threads_as_extractors_executors'],
                                max_transformation_pipelines=config['max_transformation_pipelines'],
                                use_threads_as_transformation_pipelines=config['use_threads_as_transformation_pipelines'],
                                use_threads_as_loaders_executors=config['use_threads_as_loaders_executors'],
                                trans_in_queue_max_size=config['buffer_size'],
                                extractor=FilesListExtractor(_LOGGER, intput_dir=in_dir, pattern=".txt", output_key='_'),
                                transformers=[
                                        ItemAttributeTransformer(_LOGGER, input_key_path=['_'], mappers=[os.path.abspath]),
                                        # AddStaticValuesTransformer(_LOGGER,
                                        #         static_values=[(['words_count'], 0)],
                                        #         copy_values_key_paths=[('file_path', ['_'])],
                                        #         remove_key_paths = [['_']]),
                                        ReduceItemTransformer(  _LOGGER,
                                                input_key_path=(['_'], str), 
                                                output_key='words_count', 
                                                copy_values_key_paths=[('file_path', ['_'])],
                                                transformers=[
                                                        # FileTextReaderTransformer consumes a lot of RAM but FileToTextLinesTransformer takes more time
                                                        FileToTextLinesTransformer(_LOGGER, 
                                                                pattern=".txt", 
                                                                input_key_path=None, 
                                                                output_key=None),
                                                        # TextWordTokenizerTransformer( _LOGGER, 
                                                        #         pattern="\\s+", 
                                                        #         input_key_path=['_', 'line'], 
                                                        #         output_key=None,
                                                        #         mappers=[str.strip],
                                                        #         ignore_word_fn=str.isspace),
                                                        ArabicTextWordsTokenizerTransformer( _LOGGER, 
                                                                input_key_path=['_', 'line'], 
                                                                output_key=None,
                                                                ignore_word_fn=str.isspace)
                                                            ],
                                                initial_value=0,
                                                reducer=ReduceItemTransformer.count
                                        ),                                        
                                        UniqueFilterTransformer(_LOGGER, 
                                                bag_key_path=(['file_path'], str), 
                                                unique_key_path=(['_', 'word'], str),
                                                unique_value_normalizers=[str.lower, str.strip],
                                                yield_unique_values=True,
                                                transformers=[
                                                        FileToTextLinesTransformer(_LOGGER, 
                                                                pattern=".txt", 
                                                                input_key_path=['file_path'], 
                                                                output_key='_',
                                                                copy_values_key_paths=[('file_path', ['file_path']), 
                                                                                    ('words_count', ['words_count'])]), 
                                                        # TextWordTokenizerTransformer(_LOGGER, 
                                                        #         pattern="\\s+", 
                                                        #         input_key_path=['_', 'line'], 
                                                        #         output_key='_', 
                                                        #         mappers=[str.strip],
                                                        #         ignore_word_fn=str.isspace,
                                                        #         # remove_chars = ["\ufeff"],
                                                        #         copy_values_key_paths=[('file_path', ['file_path']), 
                                                        #                             ('words_count', ['words_count'])]),
                                                        ArabicTextWordsTokenizerTransformer(_LOGGER, 
                                                                input_key_path=['_', 'line'], 
                                                                output_key='_', 
                                                                ignore_word_fn=str.isspace,
                                                                copy_values_key_paths=[('file_path', ['file_path']), 
                                                                                    ('words_count', ['words_count'])]),
                                                        ArabicRemoveDiacFromWordTransformer(_LOGGER, 
                                                                input_key_path=['_', 'word']),
                                                ]
                                        ),                                        
                                        ItemAttributeTransformer(_LOGGER, 
                                                input_key_path=['file_path'], 
                                                mappers=[basename_backwards_x3]
                                        ),
                                ],
                                loaders=[
                                        # ConditionalLoader(  _LOGGER, 
                                        #         not config['save_to_db'],
                                        #         CSV_FileLoader( _LOGGER,
                                        #                 input_key_path=None,
                                        #                 values_path=[('word', ['_', 'word'], True), 
                                        #                                 ('file', ['file_path'], True),
                                        #                                 ('words_count', ['words_count'], True)],
                                        #                 out_dir=os.path.abspath(config['out_dir']),
                                        #                 out_file_ext='txt',
                                        #                 buffer_size=config['buffer_size'])
                                        # ),
                                        # ConditionalLoader(  _LOGGER, 
                                        #         config['save_to_db'],
                                        #         #  False,
                                        #         MySQL_DBLoader( _LOGGER, **mysql_db_loader_config)
                                        # ),
                                        # NoopLoader(_LOGGER, 
                                        #         input_key_path=None, 
                                        #         log=True, 
                                        #         log_level=INFO, 
                                        #         values_path=[('word', ['_', 'word'], True), 
                                        #                     ('file', ['file_path'], True),
                                        #                     ('words_count', ['words_count'], True)],
                                        # ),
                                        ConditionalLoader(  _LOGGER, 
                                                config['save_to_db'],
                                                # False,
                                                LoadBalanceLoader(_LOGGER, 
                                                        queue_no_block_timeout_sec = 0.09,
                                                        buffer_size=config['load_balancer_buffer_size'],
                                                        use_threads_as_loaders_executors=config['use_threads_as_load_balancer_loaders_executors'],
                                                        loaders= [(
                                                                    config['buffer_size']*10, 
                                                                    MySQL_DBLoader( _LOGGER, **mysql_db_loader_config)) 
                                                                    for _ in range(0, max(1, config['load_balancer_parallel_loader_count']))]
                                                )
                                        ),
                                ]))
        LOGGER.log(INFO, '{} pipelines created by root folder in {}'.format(len(pipelines), config['in_dir']))
        for pipeline in pipelines:
            pipeline.start()
        LOGGER.log(INFO, 'Pipelines started'.format(len(pipelines)))
        block_join_threads_or_processes(pipelines, logger=LOGGER, log_level=INFO, log_when_joined=True, log_msg="Pipeline joined")
        LOGGER.log(INFO, 'Pipelines joined'.format(len(pipelines)))
    except Exception as ex:
        LOGGER.log(ERROR, "Trace : {}".format(str(traceback.format_exception(ex))))
    finally:
        end_exec_time=time.perf_counter()
        LOGGER.info('Script executed in {} sec'.format(str(round(end_exec_time-start_exec_time, 3))))


"""
SELECT COUNT(*) FROM words; 
-- 2_140_380 in 2 min/28 fichiers (12.9Mo)  ==> 0.00908430232 sec/ko
-- 676_360_080 in 9.65 hour/8848 fichiers (4Go)

-- arabic : 32_288_256 words processed in 8.25 min/01  file   324Mo (1_001 unique rows + file words count fulltext) ==> 2go RAM
-- arabic : 32_288_256 words processed in 5.61 min/01  file   324Mo (1_001 unique rows without file words count) ==> qlqs mo RAM
-- arabic : 32_288_256 words processed in 9.89 min/01  file   324Mo (1_001 unique rows + file words count by lines) ==> 700mo RAM
-- arabic : 32_288_256 words processed in 9.16 min/196 file   324Mo (1_961_96 unique rows + file words count by lines one folder)  ==> 700mo RAM
-- arabic : 32_288_256 words processed in 7.81 min/196 file   324Mo (1_961_96 unique rows + file words count by lines two folders) ==> 700mo RAM
-- ===> 0.00178855613 sec/ko
-- arabic : 407_886_336   words processed in 35 min/2_476 file     4Go   (24_78_476 unique value + file words count by lines two folders) ==> 1go RAM
-- arabic : 1_223_659_008 words processed in 1h31 min/7_428 file  12Go   (7_435_428 unique value + file words count by lines two folders) ==> 1go RAM
-- ===> 0.00050067901 sec/ko

SELECT count(DISTINCT(file_path)) FROM words;

select sum(file_words_count) FROM (SELECT DISTINCT(w.file_path), w.file_words_count FROM words AS w) AS x;

SELECT * FROM words LIMIT 1000;

SELECT concat(word, file_path), COUNT(*) AS x FROM words GROUP BY 1 HAVING X>1; 
"""
    