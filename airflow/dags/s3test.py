from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import os
import logging
from datetime import datetime, timedelta
import concurrent.futures
from modules.decorator import logger

# Modules to interact with S3 bucket
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.handlers import disable_signing

YEAR = datetime.now().strftime("%Y")
TODAY = datetime.now().strftime("%Y-%m-%d")
CLEAN_CSV_DIRECTORY = f"/mnt/shared/weather/data/clean" 
RAW_FILES_DIRECTORY = f"/mnt/shared/weather/data/raw"

def s3_upload():
    s3_hook = S3Hook(aws_conn_id='aws_sedev1_df')
    s3_hook.get_conn()
    #s3_client = s3_conn.client('s3')
    #This example uses the boto3 client
    #Client Example
    files = os.listdir(f"{RAW_FILES_DIRECTORY}/{YEAR}")
    string = f"{RAW_FILES_DIRECTORY}/{YEAR}/"
    new_files = [string + x for x in files]
    #files = s3_hook.list_keys(bucket_name='isd-weather')
    #print("BUCKET:  {}".format(new_files))
    for file in new_files:
        s3_hook.load_file(
            filename=file,
            key=file.rsplit('/', 1)[-2],
            bucket='isd-weather',
            replace=True
        )

    #objects = s3_client.list_objects_v2(Bucket="isd-weather")
#
#    for obj in objects['Contents']:
#        print(obj['Key'], obj['Size'], obj['LastModified'])
    

with DAG(
    dag_id='fail_example',
    start_date=days_ago(1),
    schedule_interval="@daily"
) as dag:

    python_upload = PythonOperator(
        task_id='start',
        python_callable=s3_upload
    )

python_upload
