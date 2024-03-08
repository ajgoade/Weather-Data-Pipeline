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
CLEAN_CSV_DIRECTORY = f"/mnt/shared/weather/data/clean" 
RAW_FILES_DIRECTORY = f"/mnt/shared/weather/data/raw"


def download_task():    
    """
    Download the objects modified within the last 24 hours inside an S3 bucket folder of current year.    
    """
    # Get the list of objects to download
    list_of_objects = noaa_isd.list_object_keys(f"{YEAR}/")
    object_keys = (obj.key for obj in noaa_isd.get_daily_list(list_of_objects))

    # Folder to save the raw files. Create it if it does not exist        
    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{YEAR}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{YEAR}")    
        os.makedirs(f"{CLEAN_CSV_DIRECTORY}/{YEAR}")
   
    # Download all the objects through multi-threading
    logging.info("Starting Download")
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", YEAR, f"{YEAR}/", f"/mnt/shared/weather/data/raw/{YEAR}/")


def s3_upload():
    s3_hook = S3Hook(aws_conn_id='aws_sedev1_df')
    s3_hook.get_conn()
    files = os.listdir(f"{RAW_FILES_DIRECTORY}/{YEAR}")
    string = f"{RAW_FILES_DIRECTORY}/{YEAR}/"
    new_files = [string + x for x in files]
    for file in new_files:
        s3_hook.load_file(
            filename=file,
            key='/'.join(file.split('/')[-2:]),
            bucket_name='isd-weather',
            replace=True
        )

################# DAG #################

# DAG configuration
local_workflow = DAG(
    "GetWeatherData",
    schedule_interval="1 0 * * *", # Run at 00:01 Everyday
    start_date = days_ago(1),    
    dagrun_timeout=timedelta(minutes=60),
    catchup=False
)

with local_workflow:  
    # Download the objects from the S3 Bucket
    task1 = PythonOperator(
        task_id = "DownloadData",
        python_callable = download_task
    )

        # Change all double quotes to single quotes
    task2 = BashOperator(
        task_id = "ExtractArchive",   
        do_xcom_push = False,     
        bash_command = f"""
        sed -i 's/"/\'/g' /mnt/shared/weather/data/raw/2024/*
        """
    )

    task3 = PythonOperator(
        task_id='UploadData',
        python_callable=s3_upload
    )


    task1 >> task2 >> task3
