import rclone_python_fixed as rclone
from rclone_python import remote_types
from enum import Enum
from pathlib import Path
import utils
import uuid
import os
import pathlib
import shutil
from datetime import datetime
import tarfile


   
class PathDontExistsInRemoteStoage(Exception):
    pass

class FileDontExistsInRemoteStoage(Exception):
    pass

class LocalPathDontExists(Exception):
    pass

class DontFoundFileOrDirectoryWithTheSpecificHash(Exception):
    pass


class Manager:
    def __init__(self,service,args_to_connect):
        self.__service=service
        self.__end_of_command=args_to_connect
        
    @staticmethod
    def __split_file_in_parts(in_path,out_path):
        fp_in=open(in_path,'rb')
        n_parts=0
        while True:
            fp_out=open(out_path+os.sep+(os.path.basename(in_path)+".part{:06d}".format(n_parts)),'wb')
            
            for i in range(1024):
                data = fp_in.read(1024*1024)  # read in 1024KB chunks
                
                if not data:
                    break
                fp_out.write(data)
            n_parts=n_parts+1
            fp_out.close()
            if not data:
                break
        fp_in.close()

    @staticmethod
    def __prepare_dir_to_upload_to_cloud(in_path,sha_256):
        date_datetime=datetime.now()
        date=date_datetime.strftime('%Y_%m_%d__%H_%M_%S')
        date=date+"_"+"{:06d}".format(int(date_datetime.strftime("%f")))

        dst_path=str((Path(in_path).parent))+os.sep+'temp_'+str(uuid.uuid4())+os.sep+os.path.basename(in_path)
        
        pathlib.Path(dst_path).mkdir(parents=True, exist_ok=True)
        dst_path_sub_dirs=dst_path+os.sep+'date='+date+os.sep+'hash='+sha_256
        pathlib.Path(dst_path_sub_dirs).mkdir(parents=True, exist_ok=True)
        Manager.__split_file_in_parts(in_path,dst_path_sub_dirs)
        return dst_path
    
    def list_all_files(self,path_to_file):        
        try:
            return rclone.ls(self.__service+path_to_file,args=[self.__end_of_command],max_depth=3,files_only=True)
        except:
            raise PathDontExistsInRemoteStoage

    
    def __check_if_file_exists_in_remote(self,hash, path_to_file_in_cloud):
        files=self.list_all_files(path_to_file_in_cloud)
        for file in files:
            if(('hash='+hash+"/") in file['Path']):
                return True
        return False

    def copy_to_cloud(self,in_path_arg,out_path):
        
        file_name=""
        in_path=in_path_arg
        if(Path(in_path).is_dir()):
            utils.tar_directory(in_path,in_path+'.tar') 
            in_path=in_path+'.tar'
            file_name=os.path.basename(in_path)
        else:
            file_name=os.path.basename(in_path)
            if(out_path[-1]=='/'):
                out_path=out_path[:-1]
        try:
            sha256=utils.calc_sha256(in_path)
        except:
            raise LocalPathDontExists
        dir_to_upload=Manager.__prepare_dir_to_upload_to_cloud(in_path,sha256)
        if(self.__check_if_file_exists_in_remote(sha256,out_path+"/"+file_name)):
            shutil.rmtree(str(Path(dir_to_upload).parent))
            return
                
        rclone._copy_move(dir_to_upload,self.__service+out_path+"/"+file_name,args=[self.__end_of_command])
        shutil.rmtree(str(Path(dir_to_upload).parent))
        if(Path(in_path_arg).is_dir()):
            os.remove(in_path)
        return sha256
    
    @staticmethod
    def get_the_newest_file_from_cloud(list_of_files):
        max_date=None
        for path in list_of_files:
            date=path['Path'].split("/")[0]
            if(max_date==None or max_date< date):
                max_date=date
            
        for path in list_of_files:
            if(max_date in path['Path'][:len(max_date)]):
                return path['Path'][:-(len(path['Name'])+1)]
        return None
    
    @staticmethod
    def concat_all_files_of_a_directory(dir_with_files):
        path_file_union=dir_with_files+os.sep+str(uuid.uuid4())
        files=os.listdir(dir_with_files)
        for file in files:
            with open(path_file_union,'wb') as fp_write:
                with open(dir_with_files+os.sep+file,'rb') as fp_read:
                    while(True):
                        data = fp_read.read(1024*1024)  # read in 1024KB chunks
                        if not data:
                            break
                        fp_write.write(data)
            os.remove(dir_with_files+os.sep+file)
        return path_file_union
    
    @staticmethod
    def handle_file_downloaded_from_cloud(dir_with_file_download_from_cloud,is_directory,name_of_file_or_directory):
        path_file_union=Manager.concat_all_files_of_a_directory(dir_with_file_download_from_cloud)
        final_path_of_the_file_or_directory=str(Path(dir_with_file_download_from_cloud).parent)
        try:
            if(is_directory):
                shutil.rmtree(final_path_of_the_file_or_directory+os.sep+name_of_file_or_directory[:-4])
            else:
                shutil.rmtree(final_path_of_the_file_or_directory+os.sep+name_of_file_or_directory)
        except:
            pass

        try:
            if(is_directory):
                os.rmdir(final_path_of_the_file_or_directory+os.sep+name_of_file_or_directory[:-4])
            else:
                os.rmdir(final_path_of_the_file_or_directory+os.sep+name_of_file_or_directory)
        except:
            pass

        if(is_directory):
            file = tarfile.open(path_file_union)
            file.extractall(str(Path(final_path_of_the_file_or_directory)))
            os.remove(path_file_union)
        else:
            shutil.move(path_file_union,str(Path(final_path_of_the_file_or_directory))+os.sep+name_of_file_or_directory)

        shutil.rmtree(dir_with_file_download_from_cloud)
        return None

    def get_file_from_hash(hash, list_of_files):
        for file in list_of_files:
            if ("hash="+hash+"/") in file['Path']:
                return file['Path'][:-(len(file['Name'])+1)]

        return None
            
    def copy_to_local(self,in_path,out_path_arg, is_directory=True, download_file_with_specific_hash=None):
        out_path=out_path_arg+os.sep+os.path.basename(in_path)
        if(is_directory):
            if(Path(out_path[:-4]).is_dir()):
                utils.tar_directory(out_path[:-4],out_path) 
            
        try:
            sha256=utils.calc_sha256(out_path)                        
        except:
            sha256=None
        
        if(is_directory):
            os.remove(out_path)

        if(sha256!=None and sha256==download_file_with_specific_hash):
            return
        
        
        list_of_files=self.list_all_files(in_path)
        if(len(list_of_files)==0):
            raise PathDontExistsInRemoteStoage
        
        if(download_file_with_specific_hash==None):
            file_path_to_download=Manager.get_the_newest_file_from_cloud(list_of_files)
            if(sha256!=None and "/hash="+sha256 in file_path_to_download):
                return 
        else:
            file_path_to_download=Manager.get_file_from_hash(download_file_with_specific_hash, list_of_files)

            if(file_path_to_download==None):
                raise DontFoundFileOrDirectoryWithTheSpecificHash
        
        orig_path__download=in_path+'/'+file_path_to_download
        dst_path=out_path_arg+os.sep+'temp_'+str(uuid.uuid4())
        
        pathlib.Path(dst_path).mkdir(parents=True, exist_ok=True)
        
        rclone._copy_move(self.__service+orig_path__download,dst_path,args=[self.__end_of_command])
        
        Manager.handle_file_downloaded_from_cloud(dst_path,is_directory,os.path.basename(in_path))
        return file_path_to_download.split('/hash=')[0].split("/")[0]
   
'''
user='admin'
passwd='12345678'
region='us-west-rack-2'
host='http://localhost:9000'
protocol=':s3:/'
args=f" --s3-access-key-id {user} --s3-secret-access-key {passwd} --s3-region {region} --s3-endpoint {host}"
#protocol=":sftp:/"
#args="--sftp-port 22  --sftp-pass as7lqWqw2ed14xBAEs2awBPqNPer5vQ --sftp-user=teste01 --sftp-host 127.0.0.1"
manager=Manager(protocol,args)
#print(manager.list_all_files('artefacts/test_split_v2'))
manager.copy_to_cloud("../artifact_manager_v2/temp2/archive.zip","artefacts/")
#manager.copy_to_local("artefacts/archive.zip","../artifact_manager_v2/temp2",is_directory=False)
#manager.copy_to_local("artefacts/temp2/temp2.tar","../artifact_manager_v2",download_file_with_specific_hash="8ba3fb56ac5cf5eaf6f860f1b3ed71d818218e5d2fc732bc6d880847a7ddb126")
#manager.copy_to_local("artefacts/temp2/temp2.tar","../artifact_manager_v2/",download_file_with_specific_hash="6c8a8a82f2f250d530774a78987348ff7a250bdf4d5e5672ce865e531c3a6309")
#manager.copy_to_local("artefacts/temp2/temp2.tar","../artifact_manager")
'''    