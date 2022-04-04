from math import ceil
import os
import shutil

def get_size_in_mo(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size/1024/1024

if __name__=="__main__":
    source_dir = './sample_data/books'
    destination_base_dir = './bdall_test_data'
    data_size_mo = 1024*4

    if not os.path.isdir(destination_base_dir):
        raise RuntimeError("Folder {} doesn't exists".format(destination_base_dir))
    destination_dir = os.path.join(destination_base_dir, '__generated')
    if not os.path.isdir(destination_dir):
        os.mkdir(destination_dir)
        print(destination_dir + ' created')

    source_dir_size = get_size_in_mo(source_dir)
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