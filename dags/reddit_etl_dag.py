import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import asyncio
from src.extract import *
from src.load import *
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'reddit_etl',
    default_args=default_args,
    description='WSB ETL for Viz/Analysis',
    schedule_interval=timedelta(days=1),
)

def wsb_etl_s3():
    posts, comments = asyncio.run(fetch_past_day())
    load_to_s3(posts, "posts")
    load_to_s3(comments, "comments")
    


initial_wsb_data = PythonOperator(
    task_id='fetch_and_upload',
    python_callable=wsb_etl_s3,
    dag=dag,
)

initial_wsb_data