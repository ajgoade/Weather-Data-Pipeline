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

def s3_upload():
    session = S3Hook("aws_sedev1_df")
    s3_client = session.client('s3')
    #This example uses the boto3 client
    #Client Example

    objects = s3_client.list_objects_v2(Bucket="isd-weather")

    for obj in objects['Contents']:
        print(obj['Key'], obj['Size'], obj['LastModified'])
    

with DAG(
    dag_id='s3cli_test_dag',
    start_date=days_ago(1),
    schedule_interval="@daily"
) as dag:

    python_upload = PythonOperator(
        task_id='start',
        python_callable=s3_upload
    )

python_upload
