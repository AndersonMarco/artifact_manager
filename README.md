# Artifact manager
Manage data science artifacts with this Python library. 

Ths library allow to use cloud storage services to manager data science artifacts like models or databases.  The
The artifacts are stored in a simple directory struct where the of the artifact is key to control the artifacts versions. The library made over **Rclone**, therefore support all services that the **Rclone** support.

## Note
All directories copied to the cloud service have the suffix **.tar** added in the file, because all directories upload to the cloud are tar files.

# How to install
```shell
git clone https://github.com/AndersonMarco/artifact_manager.git
cd artifact_manager
pip instal .
```

# Examples

How to connect to indepedent S3 server (like a local instalation of **Minio**) in the ip url **http://localhost:9000**:

```python
import  artifactmanager as am
user='user01'
passwd='123'
region='region1'
host='http://localhost:9000' #s3 host
protocol=':s3:/'
args=f" --s3-access-key-id {user} --s3-secret-access-key {passwd} --s3-region {region} --s3-endpoint {host}"
manager=am.Manager(protocol,args)
```
## Copy from local to cloud

Copy the directory **/home/user/hello_dir** to the cloud path **/artifacts**:
```python
manager.copy_to_cloud("/home/user/hello_dir","/artefacts")
```
Copy the file **/home/user/hello.txt** to the cloud path **/artifacts**:
```python
manager.copy_to_cloud("/home/user/hello.txt","/artefacts")
```
## Copy from cloud to local
### Copy the least version of a file or directory


Copy the directory the in cloud path **/artifacts/hello_dir**   to local path  **/home/user** :
```python
manager.copy_to_cloud("/artifacts/hello_dir.tar","/home/user", is_directory=True)
```

Copy the file the in cloud path **/artifacts/hello.txt**   to local path  **/home/user** :
```python
manager.copy_to_cloud("/artifacts/hello_dir.tar","/home/user", is_directory=False)
```

### Copy a specific version (defined by the hash) of a file or directory

Copy the directory the in cloud path **/artifacts/hello_dir**   to local path  **/home/user** :
```python
hash="8ba3fb56ac5cf5eaf6f860f1b3ed71d818218e5d2fc732bc6d880847a7ddb126"
manager.copy_to_cloud("/artifacts/hello_dir.tar","/home/user", is_directory=True,download_file_with_specific_hash=hash)
```
