import os
import sys

# Add the DWH directory(the parent directory) to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# appa SDK
from packages.paths import DWHPaths
from packages.upload_tools import UploadToBigQuery
from packages.extract_tools import ExtractPostgreSQL

# Dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

# Built-in packages
import pandas as pd
import numpy as np
import json
import pathlib
from time import perf_counter

# GCP 
from google.cloud import bigquery
from google.oauth2 import service_account

# Module with paths and keys
PATHS = DWHPaths()

# Script name = table name
table_name = pathlib.PurePath(__file__).stem

# Extract metadata from credentials path
with open(PATHS.EXTRACT_KEYS) as creds:
    data = json.load(creds)

username = data['username']
password = data['password']
host = data['host']
port = data['port']
database = data['database']
dialect = data['dialect']
driver = data['driver']
project_id = data['project_id_gcp']
dataset = data['dataset']

upload = UploadToBigQuery(path_credentials= PATHS.GCP_KEYS, project_id= project_id, dataset= dataset, table_bq= table_name)

def wrap_up_to_bq(df_chunk: pd.DataFrame):

    upload.load_each_df_to_bq(df_chunk= df_chunk)

def run():

    print('PIPELINE STARTED...')
    t1_start = perf_counter()

    extract = ExtractPostgreSQL(username= username, password= password, host=host, port= port, database=database, dialect=dialect, driver=driver, table=table_name)
    df_to_cast = extract.get_meta_rawdf()

    extract.cast_columns_to_floats(df_raw= df_to_cast)
    extract.cast_columns_to_strings(df_raw= df_to_cast)
    extract.cast_columns_to_bool(df_raw= df_to_cast)
    extract.cast_columns_to_datetime(df_raw= df_to_cast)

    df_to_cast.drop(labels='id', axis=1, inplace=True)

    print('Cast ready')

    df = dd.read_sql_table(table_name= table_name, index_col='id', con=f'{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}', meta=df_to_cast, bytes_per_chunk='128 MiB')
    df = df.reset_index()
    df.columns = df.columns.str.replace('index', 'id')
    df['id'] = df['id'].astype(int)

    print('Pulled down ready')

    print('Uploading started')
    upload.trim_memory()

    try:
        df.map_partitions(lambda d: wrap_up_to_bq(df_chunk= d), meta=df_to_cast).compute()
    except StopIteration:
        pass

    t1_finish = perf_counter()
    print('PIPELINE FINISHED.')

    print(f'Pipeline finished in {round(t1_finish-t1_start,2)/ 60} minutes')

def available_activities():

    run()

if __name__ == '__main__':
    available_activities()


    





