from multiprocessing import Queue, Process
import os
import signal
import uuid
import time
from utils import DATA_BY_PROCESS_CHUNK_SIZE, MAX_QUEUE_GET_TIMEOUT_SEC, run_pipelines_blocking
from utils import MAX_QUEUE_PUT_TIMEOUT_SEC, MAX_QUEUE_SIZE, PROC_JOIN_TIMEOUT
from utils import log_msg, read_arabic_words_in_txt_files
from classes import AbstractWordSaver, WordSaverFactory
from utils import remove_diac_from_word, tokenize_arabic_words_as_array_bis
from logging import INFO, WARN, ERROR
import glob

def split_list(items: list, chunk_size: int):
  for i in range(0, len(items), chunk_size):
      yield items[i:i + chunk_size]

def save_words(queue: Queue, job_uuid: str, words_saver: AbstractWordSaver):
    try:
        while True:
            words = queue.get(block=True, timeout=MAX_QUEUE_GET_TIMEOUT_SEC)
            words_saver.save_words(job_uuid, words)
    except Exception as e:
        log_msg("Queue empty {} after timeout : {}".format(job_uuid, str(e.args)), exception=e, level=ERROR)

def build_read_arabic_words_pipelines(bdall_dir: str, 
                                        tokenizer_fn, 
                                        remove_diac_from_word_fn, 
                                        words_saver_factory: WordSaverFactory, 
                                        queue_max_size, 
                                        infos: dict,
                                        has_base_dirs=False):
    pipelines = [] # [{'queue': Queue, 'save_proc': Process, 'tasks': [Process, ...]}]
    processes_count = 0
    bdall_files_count=0
    nbr_total_dom=0
    orphan_txt_files_paths=list()
    if not os.path.isdir(bdall_dir):
        raise RuntimeError("{} should be a valid directory".format(bdall_dir))

    # Corpus folders
    for corpus in os.listdir(bdall_dir):
        corpus_dir=os.path.join(bdall_dir, corpus)
        if not os.path.isdir(corpus_dir):
            if os.path.isfile(corpus_dir) and corpus_dir.endswith(".txt"):
                orphan_txt_files_paths.append(corpus_dir)
                log_msg("Txt file {} found on Corpus folder : {}".format(corpus_dir, os.path.basename(bdall_dir)))
            continue

        if not has_base_dirs:
            bases = [corpus_dir]
        else:
            bases = os.listdir(corpus_dir)

        # Base folders if there is one, otherwise it takes the value of corpus
        for base in bases:
            base_dir=os.path.join(corpus_dir, base)
            if not os.path.isdir(base_dir):
                if os.path.isfile(base_dir) and base_dir.endswith(".txt"):
                    orphan_txt_files_paths.append(base_dir)
                    log_msg("Txt file {} found on Corpus/Base folder : {}".format(base_dir, corpus))
                continue

            # Domain folders
            nbr_dom=0
            for dom in os.listdir(base_dir):
                dom_dir=os.path.join(base_dir, dom)
                if not os.path.isdir(dom_dir):
                    if os.path.isfile(dom_dir) and dom_dir.endswith(".txt"):
                        orphan_txt_files_paths.append(dom_dir)
                        log_msg("Txt file found {} on Base/Corpus folder : {}".format(dom_dir, base))
                    continue
                nbr_dom+=1
                #Pipeline
                queue = Queue(maxsize=queue_max_size)
                job_uuid = str(uuid.uuid1())
                words_saver = words_saver_factory.create(job_uuid)
                pipeline = {'queue': queue, 
                            'tasks': [], 
                            'saver': words_saver,  
                            'save_proc': Process(target=save_words, args=(queue, job_uuid, words_saver))}
                processes_count+=1

                # Period folders
                nbr_pers =0
                for per in os.listdir(dom_dir):
                    per_dir=os.path.join(dom_dir, per)
                    if not os.path.isdir(per_dir): 
                        if os.path.isfile(per_dir) and per_dir.endswith(".txt"):
                            orphan_txt_files_paths.append(per_dir)
                            log_msg("Txt file {} found on Dom folder : {}".format(per_dir, dom))
                        continue
                    nbr_pers+=1
                    files = glob.glob(per_dir + '/*.txt')
                    files_cnt = len(files)
                    bdall_files_count = bdall_files_count + files_cnt
                    files_chunks = []
                    chnks_cnt = 0
                    if files_cnt*1.3 > DATA_BY_PROCESS_CHUNK_SIZE:
                        files_chunks = split_list(files, DATA_BY_PROCESS_CHUNK_SIZE)
                    elif files_cnt > 0:
                        files_chunks=[files]
                    for chunk in files_chunks:
                        if len(chunk)>0:
                            chnks_cnt+=1
                            p1 = Process(target=read_arabic_words_in_txt_files, args=(chunk, 
                                                                                    tokenizer_fn, 
                                                                                    remove_diac_from_word_fn, 
                                                                                    {'corpus': corpus, 'domaine':dom, 'periode': per}.update(infos), 
                                                                                    queue))
                            pipeline['tasks'].append(p1)
                            processes_count+=1
                    log_msg("{} chunks found/ {} files in {}".format(chnks_cnt, len(files), per_dir))
                pipelines.append(pipeline)
            
                log_msg("{} periodes found in {}".format(nbr_pers, dom_dir))
            nbr_total_dom+=nbr_dom
            log_msg("{} domaines found in {}".format(nbr_dom, corpus_dir))
    
    # Generate processes for the orphan txt files
    orphan_chunks = split_list(orphan_txt_files_paths, DATA_BY_PROCESS_CHUNK_SIZE * 10)
    for orph_chnks in orphan_chunks:
        if len(orph_chnks)>0:
            queue = Queue(maxsize=queue_max_size)
            job_uuid = str(uuid.uuid1())
            words_saver = words_saver_factory.create(job_uuid)
            pipeline = {'queue': queue, 
                        'tasks': [], 
                        'saver': words_saver,  
                        'save_proc': Process(target=save_words, args=(queue, job_uuid, words_saver))}
            processes_count+=1
            chunks = split_list(orph_chnks, DATA_BY_PROCESS_CHUNK_SIZE)
            for chunk in chunks:
                if len(chunk)>0:
                    p1 = Process(target=read_arabic_words_in_txt_files, args=(chunk, 
                                                                                    tokenizer_fn, 
                                                                                    remove_diac_from_word_fn, 
                                                                                    {'corpus': corpus, 'domaine':dom, 'periode': per}.update(infos), 
                                                                                    queue))
                    pipeline['tasks'].append(p1)
                    processes_count+=1
            pipelines.append(pipeline)
    return (pipelines, processes_count, bdall_files_count, len(orphan_txt_files_paths))

def read_arabic_words_many_corpus_dir(bdall_dir: str, 
                                        tokenizer_fn, 
                                        remove_diac_from_word_fn, 
                                        words_saver_factory: WordSaverFactory, 
                                        queue_max_size, 
                                        show_only_summary=False,
                                        has_base_dirs=False):

    start_time = time.perf_counter()
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGINT, original_sigint_handler)

    (pipelines, processes_count, bdall_files_count, orphan_txt_files_count) = build_read_arabic_words_pipelines(bdall_dir, 
                                                                                                tokenizer_fn, 
                                                                                                remove_diac_from_word_fn, 
                                                                                                words_saver_factory, 
                                                                                                queue_max_size, 
                                                                                                {'corpus': '', 'domaine':'', 'periode': ''},
                                                                                                has_base_dirs)

    log_msg("{} processes will be started over {} total number of files ({} files found out of the standard folders)...".format(processes_count, 
                                                                                                                                bdall_files_count, 
                                                                                                                                orphan_txt_files_count), 
                                                                                                                                level=INFO)
    if show_only_summary:
        log_msg("Files & processes summary", level=INFO)
    else:
        if len(pipelines)>0:
            run_pipelines_blocking(pipelines) # this a blocking operation
        else:
            log_msg("No pipeline to run",  level=WARN)
        
        end_time = time.perf_counter()
        log_msg( "Process end in {} sec".format(round(end_time-start_time, 2)), level=INFO)


# --------------------- Program :
if __name__=="__main__":
    import time

    in_dir='bdall_test_data'
    #in_dir='bdall_real_data'
    #in_dir='bdall'
    in_dir_has_base_dirs = True # bdall > corpus > base > D > D > *.txt
    out_dir = 'out_dir'
    save_to_db=True
    db_host='localhost'
    db_name='arabic_lang'
    db_user='root'
    db_password='root'
    chunk_size=1000
    show_summary=True

    log_msg("Script started : {}".format(in_dir),  level=INFO)
    log_msg("- MAX_QUEUE_SIZE : {}".format(MAX_QUEUE_SIZE),  level=INFO)
    log_msg("- MAX_QUEUE_PUT_TIMEOUT_SEC : {}sec".format(MAX_QUEUE_PUT_TIMEOUT_SEC),  level=INFO)
    log_msg("- MAX_QUEUE_GET_TIMEOUT_SEC : {}sec".format(MAX_QUEUE_GET_TIMEOUT_SEC),  level=INFO)
    log_msg("- DATA_BY_PROCESS_CHUNK_SIZE : {}".format(DATA_BY_PROCESS_CHUNK_SIZE),  level=INFO)
    log_msg("- PROC_JOIN_TIMEOUT : {}sec".format(PROC_JOIN_TIMEOUT),  level=INFO)
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
    try:
        read_arabic_words_many_corpus_dir(os.path.abspath(in_dir), 
                                        tokenize_arabic_words_as_array_bis, 
                                        remove_diac_from_word, 
                                        WordSaverFactory(config),
                                        MAX_QUEUE_SIZE,
                                        show_only_summary=show_summary,
                                        has_base_dirs=in_dir_has_base_dirs)
    except Exception as ex:
        log_msg("Error : {}".format(str(ex.args)), exception=ex, level=ERROR)
    end_exec_time=time.perf_counter()
    log_msg('Script executed in {} sec'.format(str(round(end_exec_time-start_exec_time, 3))),  level=INFO)

# 174 domaines au total sur tous les corpus found in E:\bdall
# 784 processes will be started over 216839 (71%) total number of files (86594 fichiers ignored) : 
# # Nbr total : 303433 fichiers