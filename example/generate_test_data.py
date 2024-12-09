from math import ceil
import os
import shutil

def _get_size_in_mo(start_path = '.'):
    if os.path.isfile(start_path):
        return os.path.getsize(start_path)/1024/1024

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size/1024/1024

def duplicate_dir(source_dir, destination_base_dir, data_size_mo):
    if not os.path.isdir(destination_base_dir):
        raise RuntimeError("Folder {} doesn't exists".format(destination_base_dir))
    destination_dir = os.path.join(destination_base_dir, '__generated')
    if not os.path.isdir(destination_dir):
        os.mkdir(destination_dir)
        print(destination_dir + ' created')

    source_dir_size = _get_size_in_mo(source_dir)
    print('{} has the size of {} Mo'.format(source_dir, str(source_dir_size)))
    nbr_dirs = ceil(data_size_mo/source_dir_size)
    print('Begin. {} folder will be generated'.format(nbr_dirs))
    for i in range(0, nbr_dirs):
        dest_dir = os.path.join(destination_dir, os.path.basename(source_dir) + '_' + str(i))
        if not os.path.isdir(dest_dir):
            shutil.copytree(source_dir, dest_dir, 
                            symlinks=False, ignore=None, 
                            copy_function=shutil.copy2, 
                            ignore_dangling_symlinks=False, dirs_exist_ok=False)
            print('Generated : ' + dest_dir)
        else:
            print('Already exists : ' + dest_dir)
    print('End')

def duplicate_file(source_file, destination_base_dir, data_size_mo, dirs=1):
    if not os.path.isdir(destination_base_dir):
        raise RuntimeError("Folder {} doesn't exists".format(destination_base_dir))

    source_file_size = _get_size_in_mo(source_file)
    print('{} has the size of {} Mo'.format(source_file, str(source_file_size)))
    nbr_files = ceil(data_size_mo/source_file_size/dirs)
    print('Begin. {} files will be generated'.format(nbr_files))
    for j in range(0, dirs):
        destination_dir = os.path.join(destination_base_dir, '__generated_' + str(j+1))
        if not os.path.isdir(destination_dir):
            os.mkdir(destination_dir)
            print(destination_dir + ' created')
        for i in range(0, nbr_files):
            dest_file = os.path.join(destination_dir, str(i)+ '_' + os.path.basename(source_file))
            if not os.path.isfile(dest_file):
                shutil.copy(source_file, dest_file)
                print('Generated : ' + dest_file)
            else:
                print('Already exists : ' + dest_file)
    print('End')

if __name__=="__main__":
    # duplicate_dir('./sample_data/books', './bdall_test_data', 1024*4)
    duplicate_file('./sample_data/arabic.txt', './bdall_test_data/small_data', 4*1024, dirs=4)
    