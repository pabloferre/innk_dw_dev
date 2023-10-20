from airflow.models import DAG 
from airflow.decorators import task 
from airflow.operators.bash import BashOperator
import subprocess
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 18),
}

dag = DAG(
    'test_ETL_companies',
    default_args=default_args,
    description='ETL for companies table',
    schedule_interval='@once',
)

#Extract

extract_data = BashOperator(
    task_id='extract_data',
    bash_command='python3 ./usr/local/airflow/etl/E_companies.py',
    #xcom_push=True,
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='python3 ./usr/local/airflow/etl/T_companies.py',
    #provide_context=True,
    dag=dag
)


extract_data >> transform_data