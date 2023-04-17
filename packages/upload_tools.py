import ctypes
import gc
from google.cloud import bigquery
from google.oauth2 import service_account
from packages.paths import DWHPaths
import pandas as pd

PATHS = DWHPaths()

class UploadToBigQuery:

    def __init__(self, path_credentials: str, project_id: str, dataset: str, table_bq: str) -> None:

        self.project_id = project_id
        self.dataset = dataset
        self.table_bq = table_bq
        self.destination = f'{self.project_id}.{self.dataset}.{self.table_bq}'
        self.credentials = service_account.Credentials.from_service_account_file(path_credentials)
        self.client = bigquery.Client(project= self.project_id,credentials= self.credentials)
        self.job_config = bigquery.LoadJobConfig(autodetect= True)
        

    def trim_memory(self) -> int:

        libc = ctypes.CDLL("libc.so.6")

        return libc.malloc_trim(0) 

    def load_each_df_to_bq(self, df_chunk: pd.DataFrame):

        gc.collect()
        self.trim_memory()

        job = self.client.load_table_from_dataframe(dataframe = df_chunk, destination = self.destination, job_config= self.job_config)
        job.done()

        gc.collect()
        self.trim_memory()
        print('Table Loaded, memory released')