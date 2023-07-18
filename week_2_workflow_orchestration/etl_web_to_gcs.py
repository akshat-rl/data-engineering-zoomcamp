#!/usr/bin/env python
# coding: utf-8


import json
import urllib.parse

from time import time
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


def get_secrets(db_type, secrets_path='secrets.json'):
    """
    Get secrets credentials file
    """
    with open(secrets_path) as data_file:
        client_secrets = json.load(data_file)

        if(db_type=='mysql'):

            drivername = f'{db_type}+pymysql'
            username = client_secrets[db_type]["username"]
            password = client_secrets[db_type]["password"]
            host = client_secrets[db_type]["host"]
            port = client_secrets[db_type]["port"]
            db_name = client_secrets[db_type]["db_name"]
            
            conn_string = f"{drivername}://{username}:{urllib.parse.quote(password)}@{host}:{port}/{db_name}"

        
    return conn_string

@task(log_prints=True, retries = 3)
def get_mysql_connection(db_type, 
                         timeout_seconds=600, 
                         secrets_path='secrets.json'):
    """
    Get database connection
    """
    conn_string = get_secrets(db_type, secrets_path)
    engine = create_engine(conn_string, connect_args={"connect_timeout": timeout_seconds})
    return engine
    
@task(log_prints=True, retries = 3)
def get_worksheets_data(engine):
    query = '''select id, title, created_at from test.worksheets limit 10'''
    df = pd.DataFrame(engine.execute(query))
    return df

@task(log_prints= True, retries=3)
def clean(df):
    df['created_at'] = pd.to_datetime(df['created_at'])
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task(log_prints= True, retries = 3)
def get_worksheets_desc_data(engine):

    query = '''select id, description, grade from test.worksheets limit 10'''
    df = pd.DataFrame(engine.execute(query))
    return df


@task(log_prints= True, retries = 3)
def write_local(df):

    df.to_parquet('data/worksheets_cleaned.parquet', compression = 'gzip')
    
    return df


@task(log_prints= True, retries = 3)
def write_gcs(): 
          
    gcp_block = GcsBucket.load("zoom-de-gcs")
    #print(Path('/data/worksheets_cleaned.parquet'))

    gcp_block.upload_from_path(from_path = Path('data/worksheets_cleaned.parquet'), to_path = 'worksheets_cleaned.parquet')

    return None

@flow(name = 'data_flow')
def main_flow():
    engine = get_mysql_connection('mysql',secrets_path='secrets.json')
    print(engine)
    df_worksheets = get_worksheets_data(engine)
    df_worksheets_desc = get_worksheets_desc_data(engine)
    df_worksheets = clean(df_worksheets)
    df_worksheets = write_local(df_worksheets)
    write_gcs()
    
    

if __name__ == '__main__':
    main_flow()

    
   