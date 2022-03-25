from multiprocessing import Queue
import logging
import sys
from concurrent_log_handler import ConcurrentRotatingFileHandler
import os
from datetime import date

if not 'DEFINED' in globals():
    MAX_QUEUE_SIZE = 1000_000
    MAX_QUEUE_PUT_TIMEOUT_SEC = 30
    MAX_QUEUE_GET_TIMEOUT_SEC = 30
    DATA_BY_PROCESS_CHUNK_SIZE = 4_000
    PROC_JOIN_TIMEOUT = 10

    LOGGING_FORMAT = '%(levelname)s : %(asctime)s - %(processName)s (%(threadName)s) : %(message)s'
    logging.basicConfig(handlers=[ConcurrentRotatingFileHandler(mode="a",
                                                                        filename=os.path.abspath(f'logs/log-{date.today()}.log'),
                                                                        maxBytes=50*1024*1024, backupCount=100)], 
                                                    level=logging.DEBUG,
                                                    encoding='utf-8',
                                                    format=LOGGING_FORMAT)
    LOGGER = logging.getLogger("my-logger")
    CONSOLE_HANDLER = logging.StreamHandler(stream=sys.stdout)
    CONSOLE_HANDLER.setFormatter(logging.Formatter(LOGGING_FORMAT))
    CONSOLE_HANDLER.setLevel(logging.DEBUG)
    DEFINED=True

def log_msg(msg, console=False, error=False, level=logging.DEBUG):
    global LOGGER
    if not LOGGER is None:
        if error:
            level=logging.ERROR
        if console:
            LOGGER.addHandler(CONSOLE_HANDLER)
        LOGGER.log(level, msg)
        LOGGER.removeHandler(CONSOLE_HANDLER)
    elif console:
        print(msg)
    
def tokenize_arabic_words_as_array(txt_content) -> list:
    import re
                
    arabic_words = re.findall("[^a-zA-Z\^0-9\\^١-٩\^[()•\+-@،?!|É°¢$£\éè¡àêâ\{\}%€–_`\",ä/'''<®>*:\$\[\~\-Ò©#¬¹±²\\ª·´¶º»µ¼&¨¤¾¦¿\À´ËÂ\\¥]+", txt_content)
    all_words=[]
    # [[words.append(word) for word in txt.replace(' ', '\n').replace('\t', '\n').split('\n') if word and word.strip() ] for txt in arabic_words]
    for txt in arabic_words:
        wrds = txt.replace('×', '').replace(' ', '\n').replace('\r', '\n').replace('\t', '\n').replace('َ', '').replace('ّ', '').replace('ِ', '').replace('ُ', '').replace('ْ', '').replace('ً', '').replace('ٌ', '').replace('ٍ', '').split('\n')
        for w in wrds:
            if w and w.strip():
                all_words.append(w)
            
    return all_words

def tokenize_arabic_words_as_array_bis(txt_content) -> list:
    import re
                
    arabic_words = re.findall(r'[َُِْـًٌٍّؤائءآىإأبتثجحخدذرزسشصضطظعغفقكلمنهـوي]+', txt_content)
    all_words=[]
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

def read_file_with_encoding(filepath, expected_encoding) -> tuple:
    import cchardet as chardet
    from pathlib import Path
    file_path_ = Path(filepath)
    
    bytes_content = file_path_.read_bytes()
    detection = chardet.detect(bytes_content)
    encoding = detection['encoding']
    confidence = detection['confidence']
    text = bytes_content.decode(encoding)
    
    if expected_encoding.upper() != encoding.upper():
        log_msg("File {} has a diff. encoding {} with confid {}%. Expected {}".format(filepath, 
                                                                                    encoding, 
                                                                                    round(100*confidence, 2),
                                                                                    expected_encoding))

    return (text, encoding, confidence)

def read_arabic_words_dom_dir(dom_in_dir: str, 
                            tokenizer_fn, 
                            remove_diac_from_word_fn, 
                            prepend_per_item: list, 
                            queue: Queue) -> bool:
    import os
    import glob

    for per in os.listdir(dom_in_dir):
        per_dir=os.path.join(dom_in_dir, per)
        for txt_file in glob.glob(per_dir + '/*.txt'):
            try:
                words = read_arabic_words(txt_file, tokenizer_fn, remove_diac_from_word_fn, prepend_per_item + [per])
                if len(words) > 0:
                    queue.put(words, timeout=MAX_QUEUE_PUT_TIMEOUT_SEC)
            except Exception as e:
                log_msg("Error processing File {} : {}".format(txt_file, str(e.args)), error=True)

    return True

def read_arabic_words_in_txt_files(files_paths: list, 
                                    tokenizer_fn, 
                                    remove_diac_from_word_fn, 
                                    prepend_per_item: list, 
                                    queue: Queue) -> bool:
    for txt_file in files_paths:
        try:
            words = read_arabic_words(txt_file, tokenizer_fn, remove_diac_from_word_fn, prepend_per_item)
            if len(words) > 0:
                queue.put(words, timeout=MAX_QUEUE_PUT_TIMEOUT_SEC)
        except Exception as e:
            log_msg("Error processing File {} : {}".format(txt_file, str(e.args)), error=True)

    return True

def read_arabic_words_per_dir(per_in_dir: str, 
                                tokenizer_fn, 
                                remove_diac_from_word_fn, 
                                prepend_per_item: list, 
                                queue: Queue) -> bool:
    import glob
    return read_arabic_words_in_txt_files(glob.glob(per_in_dir + '/*.txt'),
                                            tokenizer_fn, 
                                            remove_diac_from_word_fn, 
                                            prepend_per_item, 
                                            queue)
                    
            
def read_arabic_words(txt_file_path: str, tokenizer_fn, remove_diac_from_word_fn, prepend_per_item: list)->list:
    import os
    res_words = []
    try:
        if not txt_file_path is None and not txt_file_path.endswith('.txt'):
            raise RuntimeError("Only *.txt files are allowed. File {0} given".format(txt_file_path))
            
        (txt_file_content, encod, confid) = read_file_with_encoding(txt_file_path, "utf-8")
    
        words = tokenizer_fn(txt_file_content)
        wordsCount = len(words)
        log_msg("{} words found on {}".format(wordsCount, txt_file_path))
        uniqueWords = set([remove_diac_from_word_fn(word) for word in words])
        res_words = []
        for word in uniqueWords:
            len_word = len(word)
            if(len_word < 16 and len_word > 1):  
                res_words.append(tuple([word, os.path.basename(txt_file_path), wordsCount] + prepend_per_item))
        return res_words
    except Exception as e:
        log_msg("File extracted words will be ignored due to an error {} : {}".format(txt_file_path, str(e.args)), error=True)
        return []
        #raise e
        

