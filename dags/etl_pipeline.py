import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# import boto3

# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.process_data import _process_data
from utils.fetch_data import _fetch_data

default_args = {
    'owner': 'Bikram',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'youtube_trending_videos_pipeline',
    default_args=default_args,
    description='automation for youtube trending videos',
    schedule_interval='@daily',
    start_date=datetime(2025, 5, 5),
    catchup=False,
) as dag:
    retrieve_data = PythonOperator(
        task_id='fetch_data',
        python_callable=_fetch_data,
        provide_context=True
    )
    processdata = PythonOperator(
        task_id='process_data',
        python_callable=_process_data,
        provide_context=True
    )

retrieve_data>>processdata
