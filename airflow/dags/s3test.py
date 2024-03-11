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
RAW_FILES_DIRECTORY = f"/mnt/shared/speed"

def s3_upload():
    s3_hook = S3Hook(aws_conn_id='aws_sedev1_df')
    s3_hook.get_conn()
    files = os.listdir(f"{RAW_FILES_DIRECTORY}")
    string = f"{RAW_FILES_DIRECTORY}/"
    new_files = [string + x for x in files]
    for file in new_files:
        s3_hook.load_file(
            filename=file,
            key='speed/'.join(file.split('/')[-2:]),
            bucket_name='speed-upload',
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
