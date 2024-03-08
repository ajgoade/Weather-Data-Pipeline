# Airflow Modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

# External modules
from modules import noaa_isd
import logging
from datetime import timedelta, datetime
import glob
import os

# Global variables
YEAR = datetime.now().strftime("%Y")
TODAY = datetime.now().strftime("%Y-%m-%d")
RAW_FILES_DIRECTORY = f"/mnt/shared/speed"

def s3_download():
        # Folder to save the raw files. Create it if it does not exist        
    if not os.path.exists(f"{RAW_FILES_DIRECTORY}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}")    

    s3_hook = S3Hook(aws_conn_id='aws_sedev1_df')
    s3_hook.get_conn()
    files = s3_hook.list_keys('speed-download')
    string = f"{RAW_FILES_DIRECTORY}/"

    for file in files:
        s3_hook.download_file(
            key=file,
            bucket_name=('speed-download'),
            local_path=string+file
        )

def s3_upload():
    s3_hook = S3Hook(aws_conn_id='aws_sedev1_df')
    s3_hook.get_conn()
    files = os.listdir(f"{RAW_FILES_DIRECTORY}")
    string = f"{RAW_FILES_DIRECTORY}/"
    new_files = [string + x for x in files]
    for file in new_files:
        s3_hook.load_file(
            filename=file,
            key='speed/'+file,
            bucket_name='speed-upload',
            replace=True
        )

################# DAG #################

# DAG configuration
local_workflow = DAG(
    "OoklaSpeedData",
    schedule_interval="1 0 * * *", # Run at 00:01 Everyday
    start_date = days_ago(1),    
    dagrun_timeout=timedelta(minutes=60),
    catchup=False
)

with local_workflow:  
    # Download the objects from the S3 Bucket
    task1 = PythonOperator(
        task_id = "DownloadData",
        python_callable=s3_download
    )

    task2 = PythonOperator(
        task_id='UploadData',
        python_callable=s3_upload
    )


    task1 >> task2
