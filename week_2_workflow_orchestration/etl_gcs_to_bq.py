#!/usr/bin/env python
# coding: utf-8

import os
import json
import argparse
import urllib.parse

from time import time
from datetime import timedelta
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


@task(log_prints= True, retries = 1)
def extract_from_gcs():
    gcs_path = 'worksheets_cleaned.parquet'
    gcs_block = GcsBucket.load("zoom-de-gcs")
    gcs_block.read_path(gcs_path)

    return Path(gcs_path)

@task(log_prints=True, retries = 1)
def clean_df(path):
        
        print(path)
        print("HIIII")
        df = pd.read_parquet(path)
        print(df)

        return df

@task(log_prints= True, retries = 1)
def etl_gcs_to_bq(df):
    
    gcp_credentials_block = GcpCredentials.load("zoom-de-gcs-creds")
    

    df.to_gbq (
         destination_table='zoomde.worksheets',
         project_id = 'mownica-de',
         credentials= gcp_credentials_block.get_credentials_from_service_account(),
         if_exists= 'append'
    )

    





@flow(name = 'data_flow')
def main_flow():
    local_path = 'data/worksheets_cleaned.parquet'

    path = extract_from_gcs()
    df = clean_df(local_path)
    etl_gcs_to_bq(df)
    
    

if __name__ == '__main__':
    main_flow()

    
   