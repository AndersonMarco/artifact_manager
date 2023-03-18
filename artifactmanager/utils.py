import os
import tarfile
import hashlib
from pathlib import Path
def list_all_files_of_a_directory(path):
    list_of_files=[]
    for root, dirs, files in os.walk(path):
        for filename in files:
            
            # print the complete path of the file
            file_path = os.path.join(root, filename)
            if(os.path.isfile(file_path)):
                list_of_files.append(file_path)

    return list_of_files

def tar_directory(dir_path, path_to_tar):
    dir_abs_path=os.path.abspath(dir_path)
    dir_parent=str(Path(dir_abs_path).parent)
    list_of_files= list_all_files_of_a_directory(dir_abs_path)
    list_of_files.sort()
    with tarfile.open(path_to_tar, mode='w', format=tarfile.GNU_FORMAT) as archive:        
        for file in list_of_files:            
            tarinfo=tarfile.TarInfo()
            file_path_without_dir_path=file[len(dir_parent)+1:]
            tarinfo.name=file_path_without_dir_path
            tarinfo.size=os.stat(file).st_size
            fp=open(file,'rb')            
            archive.addfile(tarinfo,fp)
            fp.close()
    
def calc_sha256(file_path):
    with open(file_path, 'rb') as f:
        # create a new SHA-256 hash object
        sha256 = hashlib.sha256()
        # read the file in chunks and update the hash object
        while True:
            data = f.read(65536)  # read in 64KB chunks
            if not data:
                break
            sha256.update(data)
    return str(sha256.hexdigest())


