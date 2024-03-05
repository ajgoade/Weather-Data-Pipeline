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
    list_of_objects = noaa_isd.list_object_keys(f"isd-lite/data/{YEAR}/")    
    object_keys = (obj.key for obj in noaa_isd.get_daily_list(list_of_objects))

    # Folder to save the raw files. Create it if it does not exist        
    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{YEAR}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{YEAR}")    
        os.makedirs(f"{CLEAN_CSV_DIRECTORY}/{YEAR}")
   
    # Download all the objects through multi-threading
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", YEAR, f"isd-lite/data/{YEAR}/", f"/mnt/shared/weather/data/raw/{YEAR}/")

################# DAG #################

# DAG configuration
local_workflow = DAG(
    "TestData",
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

    # Extract the content(s) of .gz archives
    task2 = BashOperator(
        task_id = "ExtractArchive",   
        do_xcom_push = False,     
        bash_command = f"""
        echo Found $(eval "find {RAW_FILES_DIRECTORY}/{YEAR} -name \'*.gz\' | wc -l") .gz archives in /raw/{YEAR} folder. Extracting them all now. && gunzip -fv {RAW_FILES_DIRECTORY}/{YEAR}/*.gz || true
        """
    )

    # Add the filename as prefix for every line on all the extracted text-based files
    task3 = BashOperator(
        task_id = "AddPrefix",
        do_xcom_push = False,
        bash_command = f"""
        # Get the list of files without extensions
        FILES=$(find {RAW_FILES_DIRECTORY}/{YEAR} -type f -mtime -1 ! -name "*.*")

        # Define the function to add the filename prefix to each line in place. Also, remove multiple spaces
        function add_prefix {{
            BASENAME=$(basename $1)
            PREFIX=$(echo $BASENAME | sed 's/-{YEAR}//')
            sed -i "s/^/${{PREFIX}} /" $1
            sed -i "s/[[:space:]]\+/ /g" $1
        }}

        # Export the function so it can be used by subprocesses
        export -f add_prefix

        # Use xargs to run the function in parallel on each file
        echo "$FILES" | xargs -I{{}} -P 4 bash -c 'add_prefix "$@"' _ {{}}
        """
    )

    # Combine every 500 files into a single .txt file
    task4 = BashOperator(
        task_id = "CombineFiles",
        do_xcom_push = False,
        bash_command = f"""
        find {RAW_FILES_DIRECTORY}/{YEAR} -maxdepth 1 -type f ! -name "*.*" | sort -V > file_list.txt
        split -d -l 500 file_list.txt file_list_
        for file in file_list_*; do
            echo Combining 500 files into {RAW_FILES_DIRECTORY}/{YEAR}/combined-"${{file##*_}}".txt
            cat "$file" | xargs -I{{}} bash -c 'cat {{}} >> {RAW_FILES_DIRECTORY}/{YEAR}/{TODAY}_'"${{file##*_}}".txt &            
        done
        wait

        echo "Concatenation finished"     

        rm file_list*
        find {RAW_FILES_DIRECTORY}/{YEAR} -type f -not -name "*.txt" -delete

        echo "Original raw files deleted"        
        """
    )

    task5 = HiveOperator(
        taks_id = 'create_table',
        hive_cli_conn_id='hs2_df',
        hql='CREATE TABLE IF NOT EXISTS tmp_weather(station_id varchar(14) not null,year integer,month integer,day integer,hour integer,air_temperature integer, dew_point integer, sea_lvl_pressure integer, wind_direction integer, wind_speed integer, sky_condition integer, one_hour_precipitation integer, six_hour_precipitation integer);',
    )

    task1 >> task2 >> task3 >> task4 >> task5
