

import re
import math
import os

def extract_files_from_dir(dir_abs_path: str, file_ext:str) -> list[str]:
    res_files = []
    for root_dir, dirs, files in os.walk(dir_abs_path):
        if len(files)>0:
            for file in files:
                if file.endswith(file_ext):
                    file_path = os.path.join(root_dir, file)
                    res_files.append(file_path)
    return res_files

def basename_backwards(path: str, backwards_level: int=2) -> str:
    backwards_level = max(2, backwards_level)
    paths = []
    while backwards_level>0:
        if len(path)==0 or path == '.' or path=='..':
            break
        paths.append(os.path.basename(path))
        path = os.path.dirname(path)
        backwards_level-=1

    if len(paths)==0:
        return path
    else:
        paths.reverse()
        return os.path.join(paths[0], *paths[1:])
    
def basename_backwards_x3(path: str) -> str:
    return basename_backwards(path, 4)

def basename_backwards_x4(path: str) -> str:
    return basename_backwards(path, 5)

def basename_backwards_x2(path: str) -> str:
    return basename_backwards(path, 3)

def truncate_str_255(txt: str) -> str:
    return txt if txt is None else txt[0:min(254, len(txt))]

def truncate_str_270(txt: str) -> str:
    return txt if txt is None else txt[0:min(269, len(txt))]

def len_str_gt_255(txt: str) -> bool:
    return txt if txt is None else (len(txt)>255)

def len_str_gt_270(txt: str) -> bool:
    return txt if txt is None else (len(txt)>270)

def format_duree(duree_sec: int) -> str:
    duree_sec = math.floor(duree_sec)
    d = 0
    h = int(duree_sec//3600)
    if h > 24:
        d = int(h//24)
        h = h%24
    m = int((duree_sec%3600)//60)
    s = duree_sec - (h*3600 + m*60)
    return '{}d {}h {}m {}sec'.format(d, h, m, s)
    