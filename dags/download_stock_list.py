"""
This DAG reads data from an testing API and save the downloaded data into a JSON file.
"""

import yfinance as yf
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.s3_list import S3ListOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from datetime import timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner' : 'HarryTanData.com',
    'email': ['harry.tan.data@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}


with DAG(
    dag_id =  'Download_Stock_List',
    default_args=default_args,
    description='This DAG provies samples operations in AWS S3. ',
    schedule_interval='5 5 * * *',
    start_date=days_ago(2),
    tags=['harrytandata.com'],
    catchup=False,
    max_active_runs=1,
) as dag:
    s3_sensor = S3KeySensor(
        task_id='new_s3_file',
        bucket_key='airflow/stockprices/{{ds_nodash}}/*.csv',
        wildcard_match=True,
        bucket_name='harrytandata',
        aws_conn_id='aws_s3',
        timeout=18*60*60,
        poke_interval=30,
        dag=dag)
    list_s3_file = S3ListOperator(
        task_id='list_3s_files',
        bucket='harrytandata',
        prefix='airflow/stockprices/20210901/',
        delimiter='/',
        aws_conn_id='aws_s3'
    )
    
    trigger_next_dag = TriggerDagRunOperator(
        trigger_dag_id = "Download_Stock_Price",
        task_id = "download_prices",
        execution_date = "{{ds}}",
        wait_for_completion =False
    )
    s3_sensor >> list_s3_file >> trigger_next_dag