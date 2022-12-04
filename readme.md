# Flatmapped python big data processor (ETL):

- Extract data from inputs, then dispatch each input on the transoformers chain, the output of the transformers chain is then piped to the loaders in parallel
- Transformers chain : output of each layer is processed in parallel by the next layer

# Core (uses Async) :
`EXTRACTORS |> TRANSFORMERS |> LOADERS`

This library was constructed to process Arabic text files,
But it contains many usefull ETL classes to process other text files :
### Extractors :
- `FilesListExtractor`
- `FoldersFilesListExtractor`

### Transformers :
- `OneToOneNoopTransformer`
- `FileToTextLinesTransformer`
- `FileTextReaderTransformer`
- `OneToOneItemAttributesTransformer`
- `AbstractTextWordTokenizerTransformer`
- `TextWordTokenizerTransformer`

## Loaders
- `NoopLoader`
- `ConditionalLoader`
- `CSV_FileLoader`
- `LoadBalanceLoader`
- `MySQL_DBLoader`
- `Cassandra_DBLoader`

If there is need to develop custom ETL classes, you can extend the classes :
- AbstractExtractor
- AbstractTransformer
- AbstractLoader

# Usage :

```
Help :
        python main.py options
        
        -s           Start processing
        -f           Start processing even if the estimated RAM isn't enough
        --all-cpus   Start processing using the full CPUs (default to {}% of CPUs are used)
```

Source : [V2 Github](https://github.com/enimiste/python-multiproc-bigdata/tree/pipeline/v2)

# main.py code (V2) :

```python
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
from typing import Dict, AnyStr, Any

import psutil
import argparse

from core.commons import block_join_threads_or_processes
from core.commons import get_dir_size_in_mo
from core.commons import basename_backwards_x4, format_duree, truncate_str_255, truncate_str_270
from core.commons import len_str_gt_255
from core.pipline import ThreadedPipeline
from core.extractors.files import FilesListExtractor
from core.extractors.commons import AbstractExtractor
from core.transformers.text import  TextWordTokenizerTransformer
from core.transformers.files import  FileToTextLinesTransformer, FileTextReaderTransformer
from core.transformers.one_to_one import OneToOneItemAttributesTransformer
from core.transformers.commons import OneToOneNoopTransformer
from core.transformers.aggregators import ReduceItemTransformer
from core.transformers.aggregators import UniqueFilterTransformer
from core.loaders.commons import ConditionalLoader, NoopLoader
from core.loaders.loadbalancer import LoadBalanceLoader
from core.loaders.mysql import MySQL_DBLoader
from core.loaders.files import CSV_FileLoader
from arabic_transformers import ArabicTextWordsTokenizerTransformer
from core.loaders.cassandra import Cassandra_DBLoader

LOGGING_FORMAT = '%(name)s %(levelname)s : %(asctime)s - %(processName)s (%(threadName)s) : %(message)s'
console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
console_handler.setLevel(logging.INFO)
logging.basicConfig(handlers=[ConcurrentRotatingFileHandler(mode="a",
                                                                    filename=os.path.abspath(f'logs/log-{date.today()}.log'),
                                                                    maxBytes=50*1024*1024, backupCount=100), console_handler], 
                                                level=logging.DEBUG,
                                                # encoding='utf-8',
                                                format=LOGGING_FORMAT)
LOGGER = logging.getLogger("Global")



#============================================= PARAMETRAGE :
# IN_DIR = '../bdall_test_data/__generated'
# IN_DIR = '../bdall_test_data/small_data'
IN_DIR = '../bdall_test_data/tiny_data/__files'
# IN_DIR = '../bdall_test_data/tiny_data/__generated_1'
# IN_DIR = 'E:/bdall'
SAVE_TO_DB = True
DB_HOST = 'localhost'
DB_NAME = 'words'
DB_USER = 'root'
DB_PWD = 'root'
LOAD_BALANCER_PIPES = 4
# DB_SQL_QUERIES= ["INSERT INTO allwordstemp (word, word_len, word_truncated, filename, filecount)  VALUES(%s,%s,%s,%s,%s)" 
#                                                       for _ in range(0,LOAD_BALANCER_PIPES)]
DB_SQL_QUERIES= ["INSERT INTO words (word, word_len, word_truncated, file_path, file_words_count)  VALUES(%s,%s,%s,%s,%s)" 
                                                        for _ in range(0,LOAD_BALANCER_PIPES)]
CPU_MAX_USAGE = 0.90 #0...1
MONO_PIPELINE = True
#==========================================================


def make_threaded_pipeline(extractor_: AbstractExtractor, logger: Logger, config: Dict[AnyStr, Any]):
    return ThreadedPipeline(_LOGGER, 
                use_threads_as_extractors_executors=config['use_threads_as_extractors_executors'],
                max_transformation_pipelines=config['max_transformation_pipelines'],
                use_threads_as_transformation_pipelines=config['use_threads_as_transformation_pipelines'],
                use_threads_as_loaders_executors=config['use_threads_as_loaders_executors'],
                trans_in_queue_max_size=config['trans_in_queue_max_size'],
                global_cpus_affinity_options=config['cpus_affinity_options'],
                extractor=extractor_,
                transformers=[
                        OneToOneItemAttributesTransformer(logger, trans_values_3=[(['_'], [os.path.abspath])]),
                        # OneToOneItemAttributesTransformer(logger,
                        #         static_values_1=[(['words_count'], 0), (['word_len'], 0), (['word_truncated', 0])]),
                        ReduceItemTransformer(  logger,
                                input_key_path=(['_'], str), 
                                output_key='words_count', 
                                copy_values_key_paths=[('file_path', ['_'])],
                                transformers=[
                                        # FileTextReaderTransformer consumes a lot of RAM but FileToTextLinesTransformer takes more time
                                        FileTextReaderTransformer(logger, 
                                                pattern=".txt", 
                                                input_key_path=None, 
                                                output_key=None),
                                        # TextWordTokenizerTransformer( logger, 
                                        #         pattern="\\s+", 
                                        #         input_key_path=['_', 'content'], 
                                        #         output_key=None,
                                        #         mappers=[str.strip],
                                        #         ignore_word_fn=str.isspace),
                                        ArabicTextWordsTokenizerTransformer( logger, 
                                                input_key_path=['_', 'content'], 
                                                output_key=None,
                                                ignore_word_fn=str.isspace)
                                            ],
                                initial_value=0,
                                reducer=ReduceItemTransformer.count
                        ),                                        
                        UniqueFilterTransformer(logger, 
                                bag_key_path=(['file_path'], str), 
                                unique_key_path=(['_', 'word'], str),
                                unique_value_normalizers=[str.lower, str.strip],
                                yield_unique_values=True,
                                transformers=[
                                        FileTextReaderTransformer(logger, 
                                                pattern=".txt", 
                                                input_key_path=['file_path'], 
                                                output_key='_',
                                                copy_values_key_paths=[('file_path', ['file_path']), 
                                                                    ('words_count', ['words_count'])]), 
                                        # TextWordTokenizerTransformer(logger, 
                                        #         pattern="\\s+", 
                                        #         input_key_path=['_', 'content'], 
                                        #         output_key='_', 
                                        #         mappers=[str.strip],
                                        #         ignore_word_fn=str.isspace,
                                        #         # remove_chars = ["\ufeff"],
                                        #         copy_values_key_paths=[('file_path', ['file_path']), 
                                        #                             ('words_count', ['words_count'])]),
                                        ArabicTextWordsTokenizerTransformer(logger, 
                                                input_key_path=['_', 'content'], 
                                                output_key='_', 
                                                ignore_word_fn=str.isspace,
                                                copy_values_key_paths=[('file_path', ['file_path']), 
                                                                    ('words_count', ['words_count'])]),
                                        OneToOneItemAttributesTransformer(logger, 
                                                derived_values_2=[
                                                    (['_', 'word'], ['_', 'word_len'], [ArabicTextWordsTokenizerTransformer.remove_diac, str.__len__]),
                                                    (['_', 'word'], ['_', 'word_truncated'], [len_str_gt_255]),
                                                ],
                                                trans_values_3=[
                                                    (['_', 'word'], [truncate_str_255]),
                                                ],),
                                ]
                        ),                                        
                        OneToOneItemAttributesTransformer(logger, 
                                trans_values_3=[(['file_path'], [basename_backwards_x4, truncate_str_270])]
                        ),
                ],
                loaders=[
                        # ConditionalLoader(  logger, 
                        #         not config['save_to_db'],
                        #         CSV_FileLoader( _LOGGER,
                        #                 input_key_path=None,
                        #                 values_path= config['values_to_load_path']
                        #                 out_dir=os.path.abspath(config['out_dir']),
                        #                 out_file_ext='txt',
                        #                 buffer_size=config['buffer_size'])
                        # ),
                        ConditionalLoader(  _LOGGER, 
                                config['save_to_db'],
                                #  False,
                                Cassandra_DBLoader( _LOGGER, input_key_path=None,
                                        values_path= config['values_to_load_path'],
                                        sql_query=config['db_sql_query'],
                                        db_name=config['db_name'],
                                        hosts=[config['db_host']],
                                        buffer_size=config['db_buffer_size'])
                        ),
                        # NoopLoader(_LOGGER, 
                        #         input_key_path=None, 
                        #         log=True, 
                        #         log_level=INFO, 
                        #         values_path=config['values_to_load_path'],
                        # ),
                        # ConditionalLoader(  logger, 
                        #         config['save_to_db'],
                        #         # False,
                        #         LoadBalanceLoader(logger, 
                        #                 queue_no_block_timeout_sec = 0.09,
                        #                 buffer_size=config['load_balancer_buffer_size'],
                        #                 use_threads_as_loaders_executors=config['use_threads_as_load_balancer_loaders_executors'],
                        #                 cpus_affinity_options=config['cpus_affinity_options'],
                        #                 loaders= [(
                        #                             config['load_balancer_queue_max_size'], 
                        #                             MySQL_DBLoader( logger, **{
                        #                                             'input_key_path': None, 
                        #                                             'values_path': config['values_to_load_path'],
                        #                                             'sql_query': config['db_sql_query'][i],
                        #                                             'buffer_size': config['db_buffer_size'],
                        #                                             'host': config['db_host'],
                        #                                             'database': config['db_name'],
                        #                                             'user': config['db_user'],
                        #                                             'password': config['db_password']})) 
                        #                             for i in range(0, max(1, config['load_balancer_parallel_loader_count']))]
                        #         )
                        # ),
                ])


if __name__=="__main__":
    #0.00050067901 sec/ko
    import platform
    if not platform.python_version().startswith('3.7'):
        print('This script can be executed used Python 3.7.x')
        exit()
    config = {
        'in_dir': IN_DIR,
        'out_dir': 'out_dir',
        'save_to_db': SAVE_TO_DB, 
        'db_buffer_size': 10_000,#10_000 optimal
        'db_host': DB_HOST, 
        'db_name': DB_NAME, 
        'db_user': DB_USER, 
        'db_password': DB_PWD,
        'db_sql_query': DB_SQL_QUERIES,
        'cpu_pax_usage': max(0, min(1, CPU_MAX_USAGE)),
        'cpus_affinity_options': [],# will be calculated below
        'all_cpu': '-all-cpus' in sys.argv,
        'force_run': '-f' in sys.argv,
        'start_run': '-s' in sys.argv,
        'use_threads_as_extractors_executors': False,#False optimal
        'trans_in_queue_max_size': 10_000,#10_000 optimal
        'max_transformation_pipelines': 4,#2 optimal
        'use_threads_as_transformation_pipelines': False,#False optimal
        'use_threads_as_loaders_executors': False,#False optimal
        'values_to_load_path': [('word', ['_', 'word'], True), 
                                ('word_len', ['_', 'word_len'], True),
                                ('word_truncated', ['_', 'word_truncated'], True),
                                ('file', ['file_path'], True),
                                ('words_count', ['words_count'], True)],
        'load_balancer_parallel_loader_count': LOAD_BALANCER_PIPES,#4 optimal
        'use_threads_as_load_balancer_loaders_executors': False,#False optimal
        'load_balancer_queue_max_size': 10_000,
        'load_balancer_buffer_size': 1500,#1500 optimal
        'mono_pipeline': MONO_PIPELINE,
    }

    start_exec_time = time.perf_counter()
    words_saver = None

    # Check RAM availability
    LOGGER.log(INFO, 'Calculating in_dir size ....')
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
    #region CPU affinity
    if config['cpus_affinity_options'] is None or len(config['cpus_affinity_options'])==0:
        cpus_max = 1
        if config['all_cpu']:
            cpus_max = cpus_count
        else:
            cpus_max = math.floor(cpus_count*config['cpu_pax_usage'])
        if cpus_max==0:
            cpus_max=1
        config['cpus_affinity_options'] = [0] if cpus_count == 1 else [i for i in range(0, cpus_max)]
    nbr_cpus_affinity_options = len(config['cpus_affinity_options'])
    if nbr_cpus_affinity_options==0:
        LOGGER.log(INFO, "CPU affinities options can't be empty. cpus_count*CPU_MAX_USAGE = {}".format(cpus_count*config['cpu_pax_usage']))
        exit()
    #endregion
    exec_time_sec = (0.00050067901 * 8/nbr_cpus_affinity_options) * (1+(1 - nbr_cpus_affinity_options/cpus_count)) * in_dir_size_mo * 1024 #0.00050067901 sec/ko
    nbr_processes = (1 if config['mono_pipeline'] else nbr_dirs) * nbr_processes_per_pip
    ram_per_process_mo = 100
    ram_mo = math.floor(psutil.virtual_memory()[1]/(1024*1024))
    ram_reserv_mo = 1024
    ram_secur_mo = max(0, ram_mo - ram_reserv_mo)
    estim_processes_mo = nbr_processes*ram_per_process_mo #80Mo by process
    nbr_dirs_secur = max(1, math.ceil((ram_secur_mo/ram_per_process_mo)/math.floor(nbr_processes_per_pip * 1.6))) #1.6 majoration factor
    
    
    LOGGER.log(INFO, 'Config : {}'.format(json.dumps(config, indent=4)))
    LOGGER.log(INFO, """
                        Execution rate             = 0.00050067901 sec/ko
                        Ref. CPU                   = 8 logical
                        Ref. CPU type              = 2.4Ghz, i5 11Gen
                        Ref. RAM                   = 8Go
                        _____________________________________________________________________
                        IN_DIR path                = {}
                        IN_DIR size                = {}Mo ({}Go)
                        CPU                       ~= {}
                        RAM free                   = {}Mo
                        CPUs affinity options      = {} vCpu ({}%)
                        Estimated execution time   = {} (Total : {}sec)
                        _____________________________________________________________________
                        Nbr processes python       = {}
                        RAM available              = {}Mo (RAM free - {}Mo)
                        Estimated RAM              = {}Mo ({}Mo each one) (for all processes)
                        Pipelines                  = {}
                        _____________________________________________________________________
                        Recommended root folders   = {} folders (in_dir root folders count)
                        Folders in in_dir          = {} folders
                        
                        """.format(os.path.abspath(config['in_dir']),
                                                                        in_dir_size_mo,
                                                                        math.ceil(in_dir_size_mo/1024),
                                                                        cpus_count,
                                                                        ram_mo, 
                                                                        len(config['cpus_affinity_options']),
                                                                        round(100*nbr_cpus_affinity_options/cpus_count, 2),
                                                                        format_duree(exec_time_sec),
                                                                        math.floor(exec_time_sec),
                                                                        nbr_processes, 
                                                                        ram_secur_mo, 
                                                                        ram_reserv_mo, 
                                                                        estim_processes_mo, 
                                                                        ram_per_process_mo,
                                                                        '1 (Mono pipeline)' if config['mono_pipeline'] else len(dirs),
                                                                        nbr_dirs_secur,
                                                                        nbr_dirs))
    if ram_secur_mo<estim_processes_mo:
        LOGGER.log(INFO, 'RAM not enough for running the {} processes ({}Mo each). You should structure the {} folder to have at most {} root folders, {} found'.format(
            nbr_processes, ram_per_process_mo, config['in_dir'], nbr_dirs_secur, nbr_dirs
        ))
        LOGGER.log(INFO, """
        Help :
        python main.py options
        
        -s           Start processing
        -f           Start processing even if the estimated RAM isn't enough
        --all-cpus   Start processing using the full CPUs (default to {}% of CPUs are used)
        """.format(config['cpu_pax_usage']*100))
        if not config['force_run']:
            exit()

    if not config['start_run']:
        LOGGER.log(INFO, """
        Help :
        python main.py options
        
        -s           Start processing
        -f           Start processing even if the estimated RAM isn't enough
        --all-cpus   Start processing using the full CPUs (default to {}% of CPUs are used)
        """.format(config['cpu_pax_usage']*100))
        exit()
        
    if not os.path.isdir(config['out_dir']):
        os.mkdir(config['out_dir'])
    if not os.path.isdir('logs'):
        os.mkdir('logs')
    # Start program
    LOGGER.log(INFO, "Script started")
    pipelines = []
    try:
        dirs = [os.path.abspath(os.path.join(config['in_dir'], dir)) for dir in dirs]
        if config['mono_pipeline']:
            _LOGGER = logging.getLogger("Pipeline (Unique)")
            pipelines.append(make_threaded_pipeline(extractor_=FilesListExtractor(_LOGGER, input_dir=os.path.abspath(config['in_dir']), 
                                                                                file_pattern=".txt", output_key='_'),
                                                    logger=_LOGGER, config=config))
        else:
            for (idx, dir) in enumerate(dirs):
                in_dir = os.path.join(config['in_dir'], dir)
                _LOGGER=logging.getLogger("Pipeline " + str(idx+1))
                pipelines.append(make_threaded_pipeline(extractor_=FilesListExtractor(_LOGGER, input_dir=in_dir, file_pattern=".txt", output_key='_'),
                                                        logger=_LOGGER, config=config))

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
        duree_exec = round(end_exec_time-start_exec_time, 3)
        rate = duree_exec/in_dir_size_mo/1024
        LOGGER.info('Script executed in {} sec. Rate {} sec/ko ({} Mo/sec)'.format(duree_exec, round(rate, 3), round(1/rate/1024, 3)))


```
