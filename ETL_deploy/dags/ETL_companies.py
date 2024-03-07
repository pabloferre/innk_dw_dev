from airflow.models import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 27, 12, 30),
    'retries': 5,
    'schedule_interval': '30 8 * * *',
    'max_active_runs': 1,
}

with DAG(
    'ETL_companies',
    default_args=default_args,
    description='ETL for companies table',
    catchup = False
    ) as dag:

    #Extract
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command=f'python3 /usr/local/airflow/etl/E_dim_companies.py',
        do_xcom_push=True,
        dag=dag,
    )

    #Transform
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='python3 /usr/local/airflow/etl/T_dim_companies.py "{{ ti.xcom_pull(task_ids=\'extract_data\') }}"',
        do_xcom_push=True,
        dag=dag
    )
    
    #Load
    load_data = BashOperator(
        task_id='load_data',
        bash_command='python3 /usr/local/airflow/etl/L_dim_companies.py "{{ ti.xcom_pull(task_ids=\'transform_data\') }}"',
        dag=dag
    )

    #Trigger dag users
    triger_dag_users = TriggerDagRunOperator(
        task_id='triger_dag_users',
        trigger_dag_id='ETL_users',
        wait_for_completion=True,
        dag=dag
    )



extract_data >> transform_data >> load_data >> triger_dag_users