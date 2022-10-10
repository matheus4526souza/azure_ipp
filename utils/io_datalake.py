from io import BytesIO
from main import AzureBlob
import pandas as pd
import pickle


# io_datalake main com 2 outras uma de leitura e de escrita?
class io_datalake:
    def __init__(self, path: str):
        self.azure = AzureBlob(url = path)
    
    @property
    def read_any(self) -> pd.DataFrame:
        return BytesIO(self.azure.download(self.azure.download()))
    
    def read_parquet(self) -> pd.DataFrame:
        return pd.read_parquet(self.read_any)
    
    def to_parquet(self, df: pd.DataFrame, overwrite: bool = True):
        return self.azure.upload(file=df.to_parquet(), overwrite=overwrite)
    
    def read_pickle(self) -> pd.DataFrame:
        return pickle.load(self.read_any)

    def upload_pickle(self, file: bytes, overwrite: bool = True):
        return self.azure.upload(file=pickle.dumps(file), overwrite=overwrite)
    
    
    
    
# def to_parquet(df: pd.DataFrame, path: str, overwrite=True) -> pd.DataFrame:
#     azure = AzureBlob(url = path)
#     azure.upload(file=df.to_parquet(), overwrite=overwrite)
#     return 

# def to_pickle(to_pickle: object, path: str, overwrite=True):
#     azure = AzureBlob(url = path)
#     to_pickle_azure = pickle.dumps(to_pickle)
#     azure.upload(file=to_pickle_azure, overwrite=overwrite)
#     return