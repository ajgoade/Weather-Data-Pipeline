# Airflow Modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
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
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", YEAR, f"{YEAR}/", f"/mnt/shared/weather/data/raw/{YEAR}/")

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


    task1
