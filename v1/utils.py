import codecs
import gc
from multiprocessing import Queue, Process
import logging
from logging import INFO, ERROR
from ntpath import join
import sys
import threading
from concurrent_log_handler import ConcurrentRotatingFileHandler
import os
from datetime import date
import traceback

if not 'DEFINED' in globals():
    PROC_JOIN_TIMEOUT = 1
    MAX_QUEUE_PUT_TIMEOUT_SEC = 30
    MAX_QUEUE_GET_TIMEOUT_SEC = 30
    DATA_BY_PROCESS_CHUNK_SIZE = 4_000
    MAX_QUEUE_SIZE = 10_000

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
    
    DEFINED=True

def log_msg_sync(msg: str, exception: Exception = None, level: int = logging.DEBUG):
    global LOGGER
    if not LOGGER is None:
        if not exception is None and level==ERROR:
            LOGGER.log(level, "{}, Trace : {}".format(msg, str(traceback.format_exception(exception))))
        else:
            LOGGER.log(level, msg)
        
    else:
        print(msg)
    
def log_msg(msg: str, exception: Exception = None, level: int = logging.DEBUG):
    threading.Thread(target=log_msg_sync, args=(msg, exception, level)).start()
    

def split_list(items: list, chunk_size: int):
  for i in range(0, len(items), chunk_size):
      yield items[i:i + chunk_size]

def save_words_worker(queue: Queue, job_uuid: str, words_saver):
    """
    words_saver : AbstractWordSaver
    """
    try:
        while True:
            words = queue.get(block=True, timeout=MAX_QUEUE_GET_TIMEOUT_SEC)
            words_saver.save_words(job_uuid, words)
    except Exception as e:
        log_msg("Queue empty {} after timeout : {}".format(job_uuid, str(e.args)), exception=e, level=ERROR)

def pipeline_builder(job_uuid: str, queue: Queue, words_saver) -> dict:
    """
    words_saver : AbstractWordSaver
        {'queue': queue, 
        'tasks': [], 
        'resource': words_saver,  
        'process': saver_process}
    """
    return {'queue': queue, 
            'tasks': [], 
            'resource': words_saver,  
            'process': Process(target=save_words_worker, args=(queue, job_uuid, words_saver))}

         
def read_file_with_encoding(filepath, expected_encoding) -> list[str]:
    res = []
    with codecs.open(filepath, mode="r", encoding=expected_encoding) as fh:
        res= fh.readlines()
    return res

def run_pipelines_blocking(pipelines: list):
    try:
        # Start processes
        log_msg("Start processes",  level=INFO)
        nbr_proc=0
        for p in pipelines:
            sp = p['process']
            try:
                sp.start()
                nbr_proc+=1
                one_started = False
                for t in p['tasks']:
                    try:
                        t.start()
                        one_started=True
                        nbr_proc+=1
                    except Exception as ex:
                        log_msg("Error while starting task {} : {}".format(type(t).__name__, str(ex.args)), exception=ex, level=ERROR)
                if not one_started:
                    sp.terminate()
                    p['queue'].close()
            except Exception as ex2:
                log_msg("Error while starting pipeline {} : {}".format(type(sp).__name__, str(ex2.args)), exception=ex2, level=ERROR)
        log_msg("{} processes started".format(nbr_proc),  level=INFO)
        # Joining processes
        log_msg("Joining processes",  level=INFO)
        working = True
        joined = set()
        while working:
            working = False
            for p in pipelines:
                #Save process
                sp = p['process']
                try:
                    if not sp.pid in joined:
                        sp.join(timeout=PROC_JOIN_TIMEOUT)
                        if not sp.exitcode is None:
                            joined.add(sp.pid)
                            log_msg("{} total {}(e)s joined at now".format( len(joined), type(sp).__name__),  level=INFO)
                            try:
                                sp.terminate()
                            except Exception as tex:
                                log_msg("Error while terminate tasks {} : {}".format(type(sp).__name__, str(tex.args)), level=ERROR, exception=tex)
                        else:
                            working=True

                    # Tasks
                    for t in p['tasks']:
                        try:
                            x_id = None
                            if type(t) is threading.Thread:
                                x_id = t.getName()
                            else:
                                x_id = t.pid

                            if not x_id in joined:
                                t.join(timeout=PROC_JOIN_TIMEOUT)
                                is_joined = None
                                if type(t) is threading.Thread:
                                    is_joined = not t.is_alive()
                                else:
                                    is_joined = t.exitcode is not None
                                if is_joined:
                                    joined.add(x_id)
                                    log_msg("{} total {}(e)s joined at now".format(len(joined), type(t).__name__),  level=INFO)
                                    try:
                                        sp.terminate()
                                    except Exception as tex:
                                        log_msg("Error while terminate tasks {} : {}".format(type(t).__name__, str(tex.args)), exception=tex, level=ERROR)
                                else:
                                    working=True
                        except Exception as ex:
                            log_msg("Error while joining task {} : {}".format(type(t).__name__, str(ex.args)), exception=ex, level=ERROR)
                except Exception as ex2:
                    log_msg("Error while joining pipeline {} : {}".format(type(sp).__name__, str(ex2.args)), exception=ex2, level=ERROR)
        log_msg("Closing saver objects",  level=INFO)
        # Closing saver object
        for p in pipelines:
            try:
                p['resource'].close()
            except Exception:
                pass
    except KeyboardInterrupt:
        log_msg("Caught KeyboardInterrupt, terminating workers ...", level=INFO)
        for p in pipelines:
            try:
                p['process'].terminate()
                p['resource'].close()
                for t in p['tasks']:
                    try:
                        if type(t) is threading.Thread:
                            t._stop()
                        else:
                            t.terminate()
                    except Exception as ex:
                        log_msg("Error while terminate tasks process : {}".format(str(ex.args)), exception=ex, level=ERROR)
                p['queue'].close()
            except Exception as ex2:
                log_msg("Error while terminate save process or/and queue : {}".format(str(ex2.args)), exception=ex2, level=ERROR)
    finally:
        log_msg("run_pipelines_blocking End executing",  level=INFO)

# ======================================= Metier ========================= :                   
def tokenize_arabic_words_as_array(txt_lines) -> list:
    import re
    
    txt_content = ' '.join(txt_lines)
    all_words=[]   
    arabic_words = re.findall("[^a-zA-Z\^0-9\\^١-٩\^[()•\+-@،?!|É°¢$£\éè¡àêâ\{\}%€–_`\",ä/'''<®>*:\$\[\~\-Ò©#¬¹±²\\ª·´¶º»µ¼&¨¤¾¦¿\À´ËÂ\\¥]+", txt_content)
    # [[words.append(word) for word in txt.replace(' ', '\n').replace('\t', '\n').split('\n') if word and word.strip() ] for txt in arabic_words]
    for txt in arabic_words:
        wrds = txt.replace('×', '').replace(' ', '\n').replace('\r', '\n').replace('\t', '\n').replace('َ', '').replace('ّ', '').replace('ِ', '').replace('ُ', '').replace('ْ', '').replace('ً', '').replace('ٌ', '').replace('ٍ', '').split('\n')
        for w in wrds:
            if w and w.strip():
                all_words.append(w)
    
    return all_words

def tokenize_arabic_words_as_array_bis(txt_lines) -> list:
    import re
    txt_content = ' '.join(txt_lines)
    all_words=[]
    arabic_words = re.findall(r'[َُِْـًٌٍّؤائءآىإأبتثجحخدذرزسشصضطظعغفقكلمنهـوي]+', txt_content)
    # [[words.append(word) for word in txt.replace(' ', '\n').replace('\t', '\n').split('\n') if word and word.strip() ] for txt in arabic_words]
    for txt in arabic_words:
        wrds = txt.replace('×', '').replace(' ', '\n').replace('\r', '\n').replace('\t', '\n').split('\n')
        for w in wrds:
            if w and w.strip():
                all_words.append(w)
    
    return all_words

def remove_diac_from_word(word) -> str:
   # import re
    word = word.replace('َ', '').replace('ّ', '').replace('ِ', '').replace('ُ', '').replace('ْ', '').replace('ً', '').replace('ٌ', '').replace('ٍ', '')
    return word


def read_arabic_words_in_txt_files(files_paths: list, 
                                    tokenizer_fn, 
                                    remove_diac_from_word_fn, 
                                    infos: dict, 
                                    queue: Queue) -> bool:
    for txt_file in files_paths:
        try:
            words = read_arabic_words(txt_file, tokenizer_fn, remove_diac_from_word_fn, infos)
            if len(words) > 0:
                queue.put(words, timeout=MAX_QUEUE_PUT_TIMEOUT_SEC)
        except Exception as e:
            log_msg("Error processing File {} : {}".format(txt_file, str(e.args)), exception=e, level=ERROR)

    return True

def read_arabic_words_per_dir(per_in_dir: str, 
                                tokenizer_fn, 
                                remove_diac_from_word_fn, 
                                infos: dict, 
                                queue: Queue) -> bool:
    import glob
    return read_arabic_words_in_txt_files(glob.glob(per_in_dir + '/*.txt'),
                                            tokenizer_fn, 
                                            remove_diac_from_word_fn, 
                                            infos, 
                                            queue)
                    
            
def read_arabic_words(txt_file_path: str, tokenizer_fn, remove_diac_from_word_fn, infos: dict)->list:
    import os
    res_words = []
    try:
        if not txt_file_path is None and not txt_file_path.endswith('.txt'):
            raise RuntimeError("Only *.txt files are allowed. File {0} given".format(txt_file_path))
            
        txt_file_lines = read_file_with_encoding(txt_file_path, "utf-8")
    
        words = tokenizer_fn(txt_file_lines)
        wordsCount = len(words)
        log_msg("{} words found on {}".format(wordsCount, txt_file_path))
        uniqueWords = set([remove_diac_from_word_fn(word) for word in words])
        res_words = []
        for word in uniqueWords:
            len_word = len(word)
            if(len_word < 16 and len_word > 1):  
                res_words.append(tuple([word, os.path.basename(txt_file_path), wordsCount] + [infos]))
        return res_words
    except Exception as e:
        log_msg("File extracted words will be ignored due to an error {} : {}".format(txt_file_path, str(e.args)), exception=e, level=ERROR)
        return []
        #raise e
        