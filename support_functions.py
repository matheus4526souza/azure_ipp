from main import AzureBlob
import pandas as pd
import pickle


# if not endswith?
def to_parquet(df: pd.DataFrame, path: str, overwrite=True) -> pd.DataFrame:
    azure = AzureBlob(url = path)
    azure.upload(df=df.to_parquet(), overwrite=overwrite)
    return 

def to_pickle(to_pickle: object, path: str, overwrite=True):
    azure = AzureBlob(url = path)
    to_pickle_azure = pickle.dumps(to_pickle)
    azure.upload(df=to_pickle_azure, overwrite=overwrite)
    return