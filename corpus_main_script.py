"""
pip install mysql-connector-python
pip install concurrent-log-handler
"""
from multiprocessing import Queue, Process
from ntpath import join
import os
import signal
import uuid
import time
import logging
from utils import DATA_BY_PROCESS_CHUNK_SIZE, MAX_QUEUE_GET_TIMEOUT_SEC
from utils import MAX_QUEUE_PUT_TIMEOUT_SEC, MAX_QUEUE_SIZE, PROC_JOIN_TIMEOUT
from utils import log_msg, read_arabic_words_in_txt_files
from classes import AbstractWordSaver, WordSaverFactory
from utils import remove_diac_from_word, tokenize_arabic_words_as_array_bis

def split_list(items: list, chunk_size: int) -> list:
  for i in range(0, len(items), chunk_size):
      yield items[i:i + chunk_size]

def save_words(queue: Queue, job_uuid: str, words_saver: AbstractWordSaver):
    try:
        while True:
            words = queue.get(block=True, timeout=MAX_QUEUE_GET_TIMEOUT_SEC)
            words_saver.save_words(job_uuid, words)
    except Exception as e:
        log_msg("Queue empty {} after timeout : {}".format(job_uuid, str(e)))

def read_arabic_words_many_corpus_dir(bdall_dir: str, 
                                        tokenizer_fn, 
                                        remove_diac_from_word_fn, 
                                        words_saver_factory: WordSaverFactory, 
                                        queue_max_size, 
                                        show_only_summary=False,
                                        has_base_dirs=False):
    import glob

    start_time = time.perf_counter()
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGINT, original_sigint_handler)

    pipelines = [] # [{'save_proc': Process, 'tasks': [Process, ...]}]
    processes_count = 0
    bdall_files_count=0
    nbr_total_dom=0
    ignored_txt_files_count=0
    if not os.path.isdir(bdall_dir):
        raise RuntimeError("{} should be a valid directory".format(bdall_dir))

    for corpus in os.listdir(bdall_dir):
        corpus_dir=os.path.join(bdall_dir, corpus)
        if not os.path.isdir(corpus_dir):
            if os.path.isfile(corpus_dir) and corpus_dir.endswith(".txt"):
                ignored_txt_files_count+=1
            log_msg("Corpus {} path ignored".format(corpus_dir))
            continue
        if not has_base_dirs:
            bases = [corpus_dir]
        else:
            bases = os.listdir(corpus_dir)

        for base in bases:
            base_dir=os.path.join(corpus_dir, base)
            if not os.path.isdir(base_dir):
                if os.path.isfile(base_dir) and base_dir.endswith(".txt"):
                    ignored_txt_files_count+=1
                log_msg("Base {} path ignored".format(base_dir))
                continue
            nbr_dom=0
            for dom in os.listdir(base_dir):
                dom_dir=os.path.join(base_dir, dom)
                if not os.path.isdir(dom_dir):
                    if os.path.isfile(dom_dir) and dom_dir.endswith(".txt"):
                        ignored_txt_files_count+=1
                    log_msg("Dom {} path ignored".format(dom_dir))
                    continue
                nbr_dom+=1
                #Pipeline
                queue = Queue(maxsize=queue_max_size)
                job_uuid = str(uuid.uuid1())
                words_saver = words_saver_factory.create(job_uuid)
                pipeline = {'queue': queue, 'tasks': [], 'saver': words_saver,  'save_proc': Process(target=save_words, args=(queue, job_uuid, words_saver))}
                processes_count+=1
                nbr_pers =0
                for per in os.listdir(dom_dir):
                    per_dir=os.path.join(dom_dir, per)
                    if not os.path.isdir(per_dir): 
                        if os.path.isfile(per_dir) and per_dir.endswith(".txt"):
                            ignored_txt_files_count+=1
                        log_msg("Per {} path ignored".format(per_dir))
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
                        chnks_cnt+=1
                        p1 = Process(target=read_arabic_words_in_txt_files, args=(chunk, 
                                                                                tokenizer_fn, 
                                                                                remove_diac_from_word_fn, 
                                                                                [corpus, dom, per], 
                                                                                queue))
                        pipeline['tasks'].append(p1)
                        processes_count+=1
                    log_msg("{} chunks found/ {} files in {}".format(chnks_cnt, len(files), per_dir))
                pipelines.append(pipeline)
            
                log_msg("{} periodes found in {}".format(nbr_pers, dom_dir), console=True)
            nbr_total_dom+=nbr_dom
            log_msg("{} domaines found in {}".format(nbr_dom, corpus_dir), console=True)
    log_msg("{} domaines au total sur tous les corpus found in {}".format(nbr_total_dom, bdall_dir), console=True)
                
    log_msg(console=True, msg="{} processes will be started over {} total number of files ({} fichiers ignored)...".format(processes_count, bdall_files_count, ignored_txt_files_count))
    if show_only_summary:
        raise RuntimeError("Files & processes summary")
    
    run_pipelines_blocking(pipelines) # this a blocking operation
    
    end_time = time.perf_counter()
    log_msg(console=True, msg="Process end in {} sec".format(round(end_time-start_time, 2)))

def run_pipelines_blocking(pipelines: list):
    try:
        # Start processes
        log_msg("Start processes", console=True)
        nbr_proc=0
        for p in pipelines:
            try:
                p['save_proc'].start()
                nbr_proc+=1
                one_started = False
                for t in p['tasks']:
                    try:
                        t.start()
                        one_started=True
                        nbr_proc+=1
                    except Exception as ex:
                        log_msg("Error while starting task process : {}".format(str(ex)), error=True)
                if not one_started:
                    p['save_proc'].terminate()
                    p['queue'].close()
            except Exception as ex2:
                log_msg("Error while starting pipeline process : {}".format(str(ex2)), error=True)
        log_msg("{} processes started".format(nbr_proc), console=True)
        # Joining processes
        log_msg("Joining processes", console=True)
        working = True
        joined = set()
        while working:
            working = False
            for p in pipelines:
                try:
                    #Save process
                    sp = p['save_proc']
                    if not sp.pid in joined:
                        sp.join(timeout=PROC_JOIN_TIMEOUT)
                        if not sp.exitcode is None:
                            joined.add(sp.pid)
                            log_msg("{} processes were joined".format(len(joined)), console=True)
                        else:
                            working=True

                    # Tasks
                    for t in p['tasks']:
                        try:
                            if not t.pid in joined:
                                t.join(timeout=PROC_JOIN_TIMEOUT)
                                if not t.exitcode is None:
                                    joined.add(t.pid)
                                    log_msg("{} processes were joined".format(len(joined)), console=True)
                                else:
                                    working=True
                        except Exception as ex:
                            log_msg("Error while joining task process : {}".format(str(ex)), error=True)
                except Exception as ex2:
                    log_msg("Error while joining pipeline process : {}".format(str(ex2)), error=True)
        log_msg("Closing saver objects", console=True)
        # Closing saver object
        for p in pipelines:
            try:
                p['saver'].close()
            except Exception:
                pass
    except KeyboardInterrupt:
        log_msg(error=True, console=True, msg="Caught KeyboardInterrupt, terminating workers ...")
        for p in pipelines:
            try:
                p['save_proc'].terminate()
                p['saver'].close()
                for t in p['tasks']:
                    try:
                       t.terminate()
                    except Exception as ex:
                        log_msg("Error while terminate tasks process : {}".format(str(ex)), error=True)
                p['queue'].close()
            except Exception as ex2:
                log_msg("Error while terminate save process or/and queue : {}".format(str(ex2)), error=True)
    finally:
        log_msg("run_pipelines_blocking End executing", console=True)

# --------------------- Program :
if __name__=="__main__":
    import time

    #in_dir='bdall_test_data'
    #in_dir='bdall_real_data'
    in_dir='bdall'
    in_dir_has_base_dirs = True # bdakk > corpus > base > D > D > *.txt
    out_dir = 'out_dir'
    save_to_db=True
    db_host='localhost'
    db_name='arabic_lang'
    db_user='root'
    db_password='root'
    chunk_size=1000
    show_summary=False

    log_msg("Script started : {}".format(in_dir), console=True)
    log_msg("- MAX_QUEUE_SIZE : {}".format(MAX_QUEUE_SIZE), console=True)
    log_msg("- MAX_QUEUE_PUT_TIMEOUT_SEC : {}sec".format(MAX_QUEUE_PUT_TIMEOUT_SEC), console=True)
    log_msg("- MAX_QUEUE_GET_TIMEOUT_SEC : {}sec".format(MAX_QUEUE_GET_TIMEOUT_SEC), console=True)
    log_msg("- DATA_BY_PROCESS_CHUNK_SIZE : {}".format(DATA_BY_PROCESS_CHUNK_SIZE), console=True)
    log_msg("- PROC_JOIN_TIMEOUT : {}sec".format(PROC_JOIN_TIMEOUT), console=True)
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
        log_msg("Error : {}".format(str(ex)), console=True, error=True)
    end_exec_time=time.perf_counter()
    log_msg('Script executed in {} sec'.format(str(round(end_exec_time-start_exec_time, 3))), console=True)

# 174 domaines au total sur tous les corpus found in E:\bdall
# 784 processes will be started over 216839 (71%) total number of files (86594 fichiers ignored) : 
# # Nbr total : 303433 fichiers