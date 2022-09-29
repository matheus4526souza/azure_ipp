#  https://ippstglabbatch.blob.core.windows.net/atena-lab/dimensions/

from dataclasses import dataclass, field
import os
from typing import Callable, Tuple
import pandas as pd
from azure.storage.filedatalake import (DataLakeServiceClient,
                                        DataLakeFileClient,
                                        FileSystemClient, 
                                        DataLakeDirectoryClient)
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import threading, time
import tqdm
from io import BytesIO
load_dotenv('.env')

@dataclass
class AzureBlob:
    
    slots = ('url', 'credential', 
             'service_client', 'file_name', 
             'container_name', '_directory')
    
    url: str = field(init=True)
    credential: DefaultAzureCredential = DefaultAzureCredential()
    
    def __post_init__(self):
        account_url, self.url, self.container_name = self.get_correct_url(self.url)
        self.service_client = DataLakeServiceClient(account_url=account_url, 
                                                    credential=self.credential)
        assert self.directory_name != self.file_name, 'The url must contain a directory and a file name'
        
    def get_correct_url(self, url: str) -> Tuple[str, str]:
        if not url.startswith('https://'):
            if url.startswith('abfs://'):
                url = 'https://' + url.split('//')[-1]
            else:
                url = 'https://' + url
        container_name = url.split('.')[0].split('//')[-1]
        if url.split('.', 2)[1] != 'dfs':
            url = url.split('.', 1)[0] + '.dfs.' + url.split('.', 2)[-1]
        
        account_url = '/'.join(url.split('/')[:3])
        
        return account_url, url, container_name
    
    @property
    def get_file_system_name(self) -> str:
        return self.url.split('/')[3]
    
    @property
    def directory_name(self) -> str:
        return self.url.split('/', 4)[-1].rsplit('/', 1)[0]
    
    @property
    def file_name(self) -> str:
        return self.url.rsplit('/', 1)[-1]
    
    def storage_client(self) -> Tuple[DataLakeFileClient, DataLakeDirectoryClient, FileSystemClient]:
        
        file_system_client = self.service_client.get_file_system_client(file_system=self.get_file_system_name)
        directory_client = file_system_client.get_directory_client(directory=self.directory_name)
        file_client = directory_client.get_file_client(file=self.file_name)
        
        return file_client, directory_client, file_system_client
    
    def chunks_generator(self, file: pd.DataFrame, chunk_size: int=1000000) -> pd.DataFrame:
        if not isinstance(file, pd.DataFrame):
            raise ValueError('The file must be a pandas DataFrame not a {}'.format(type(file)))
        file: BytesIO = BytesIO(file.to_parquet())
        file_server, directory_server, file_system_server = self.storage_client()
        if not file_server.exists():
            directory_server.create_file(self.file_name)
        else:
            file_server.delete_file()
            directory_server.create_file(self.file_name)
        filesize_previous = 0
        with tqdm.tqdm(total=file.getbuffer().nbytes,
                       unit='B', 
                       unit_scale=True, 
                       unit_divisor=1024,
                       bar_format='{l_bar}{bar:10}{r_bar}{bar:-10b}') as pbar:
            while True:
                data = file.read(chunk_size)
                if not data:
                    break
                file_server.append_data(data=data, offset=filesize_previous, length=len(data))
                file_server.flush_data(filesize_previous+len(data))
                filesize_previous += len(data)
                pbar.update(len(data))
        return

    def upload(self, 
               file: bytes, 
               overwrite: bool=True,
               file_function: Callable=None):
        
        file_client, _, _ = self.storage_client()
        if file_function:
            file = file_function(file)
        else:
            if not isinstance(file, bytes):
                file = BytesIO(data=file)
        file_client.upload_data(data=file, overwrite=overwrite)
        return


    def download(self) -> pd.DataFrame:
        file_client, _, _ = self.storage_client()
        return file_client.download_file().readall()
    
    def read_download(self) -> pd.DataFrame:
        return
    
    def get_size(self):
        file_client, _, _ = self.storage_client()
        file_client = file_client.get_file_properties()
        return round(file_client.size/1.049e6, 2)

df = pd.read_parquet('pandas_false_data.parquet')
path = os.getenv('MATHEUS_DATALAKE') 
a = AzureBlob(url = os.path.join(path, 'pandas_files/uploading_pandas_parquet.parquet'))
a.upload(file=df.to_parquet(), overwrite=True)