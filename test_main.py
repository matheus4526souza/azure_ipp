import os
import unittest
from utils.utils import get_env
import main
import pandas as pd
from dotenv import load_dotenv
load_dotenv('azure_lib/.env')


class TestAzureLib(unittest.TestCase):
    def test_azure_lib(self):
        self.df = pd.DataFrame({'a': [1, 2, 3] * 100, 'b': [4, 5, 6] * 100})
        path = get_env('MAIN_DATALAKE')
        c = main.AzureBlob(url=os.path.join(path, 'testing/test_parquet.parquet'))
        c.upload(file=self.df, overwrite=True)
        self.assertTrue(c.exists())
    