# Import the necessary libraries
import pysftp
import logging
import configparser
import json
from google.cloud import storage
from google.oauth2 import service_account
import click

# Load the parameters from the config file
config = configparser.ConfigParser()
config.read('config.cfg.template', encoding='utf-8-sig')
vm_ext_ip              =  config['GCP']['HOST_NAME']
vm_user                =  config['GCP']['USERNAME']
vm_user_pass           =  config['GCP']['PASSWORD']
gcp_credential_path    =  config['GCP']['CREDENTIAL_PATH']
gcp_bucket_name        =  config['GCP']['BUCKET_NAME']
gcp_project_name       =  config['GCP']['PROJECT_ID']


class SFTP_Server():
    """
    This class is used to initialize the SFTP server and start the server connection
    """
    def __init__(self, host_name, user_name, pass_word):
        self.host_name = host_name
        self.user_name = user_name
        self.pass_word = pass_word
        
    def connect(self):
        """
        This class method is used to connect to the SFTP server
        """
        
        try:
            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            sftp = pysftp.Connection(host=self.host_name, username=self.user_name, password=self.pass_word, cnopts=cnopts)
            return sftp
    
        except Exception:
            logging.exception("An error occured. Check the credentials")    


def gcs_auth(credential_path: str, bucket_name: str, project_id: str):
    """
    This function authenticates to GCP with the supplied credentials and returns the GCS bucket
        
    inputs:  
        
    credential_path: The path to the json file generated from GCP for the service account secret key.

    bucket_name: The name of the GCS bucket that data will be ingested into.

    project_id: Name of the project on GCP hosting the service account.

    Output: GCS bucket
    """

    with open(credential_path) as source:
        info = json.load(source)

    storage_credentials = service_account.Credentials.from_service_account_info(info)

    storage_client = storage.Client(project=project_id, credentials=storage_credentials)
        
    bucket = storage_client.get_bucket(bucket_name)
        
    return bucket

@click.command()
@click.option("--file_ext", prompt="Enter the file format (e.g. txt)", default='csv')
@click.option("--remote_folder", prompt="Enter the remote folder path for the server", default='root')
@click.option("--gcs_folder", prompt="Enter the GCS folder name")
def gcs_file_upload(file_ext, remote_folder, gcs_folder):
    """
    This function uploads files from SFTP server working directory to GCS blob storage.

    Input:

    file_ext: The extension of the files to be uploaded to GCS.
    """
    if remote_folder == 'root':
        work_dir = sftp.pwd
        file_list = sftp.listdir(work_dir)
        bucket = gcs_auth(credential_path=gcp_credential_path, bucket_name=gcp_bucket_name, project_id=gcp_project_name)
        
        for i in file_list:
            if i.split(".")[-1] == file_ext:
                blob = bucket.blob(f'{gcs_folder}/' + i)
        
                with sftp.open(i, bufsize=32768) as file:
                    blob.upload_from_file(file)

    else:
        sftp.cwd(f'./{remote_folder}')
        work_dir = sftp.pwd
        file_list = sftp.listdir(work_dir)
        bucket = gcs_auth(credential_path=gcp_credential_path, bucket_name=gcp_bucket_name, project_id=gcp_project_name)
        
        for i in file_list:
            if i.split(".")[-1] == file_ext:
                blob = bucket.blob(f'{gcs_folder}/' + i)
        
                with sftp.open(i, bufsize=32768) as file:
                    blob.upload_from_file(file)


if __name__ == '__main__':
    # Intantiate the remote server
    server = SFTP_Server(vm_ext_ip, vm_user, vm_user_pass)

    # Connect to the remote server
    sftp = server.connect()

    # Upload files to GCS
    gcs_file_upload()

    # Close the SFTP server connection
    sftp.close()