from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import os
import logging
from datetime import datetime, timedelta
import concurrent.futures
from modules.decorator import logger

YEAR = datetime.now().strftime("%Y")
TODAY = datetime.now().strftime("%Y-%m-%d")
CLEAN_CSV_DIRECTORY = f"/mnt/shared/weather/data/clean" 
RAW_FILES_DIRECTORY = f"/mnt/shared/speed"

def s3_dup():
    s3_hook = S3Hook(aws_conn_id='aws_sedev1_df')
    s3_hook.get_conn()
    files = s3_hook.list_keys('speed-download')
    string = f"{RAW_FILES_DIRECTORY}/"

    for file in files:
        s3_hook.copy_object(
            source_bucket_key=file,
            dest_bucket_key='speed/'+file,
            source_bucket_name=('speed-download'),
            dest_bucket_name=('speed-upload')
        )

with DAG(
    dag_id='S3Dup',
    start_date=days_ago(1),
    schedule_interval="@daily"
) as dag:

    python_upload = PythonOperator(
        task_id='DupObj',
        python_callable=s3_dup
    )

python_upload
