from airflow.models import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 18),
    'retries': 5,
    'schedule_interval': None,
    'max_active_runs': 1,
}

with DAG(
    'ETL_users',
    default_args=default_args,
    description='ETL for users table',
    catchup = False
    ) as dag:

    #Extract

    extract_data = BashOperator(
        task_id='extract_data',
        bash_command=f'python3 /usr/local/airflow/etl/E_dim_users.py',
        do_xcom_push=True,
        dag=dag,
    )

    #Transform
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='python3 /usr/local/airflow/etl/T_dim_users.py "{{ ti.xcom_pull(task_ids=\'extract_data\') }}"',
        do_xcom_push=True,
        dag=dag
    )

    #Load

    load_data = BashOperator(
        task_id='load_data',
        bash_command='python3 /usr/local/airflow/etl/L_dim_users.py "{{ ti.xcom_pull(task_ids=\'transform_data\') }}"',
        dag=dag
    )
    
    #Trigger dag goals
    trigger_dag_goals = TriggerDagRunOperator(
        task_id='trigger_dag_goals',
        trigger_dag_id='ETL_goals',
        wait_for_completion=True,
        dag=dag
    )





extract_data >> transform_data >> load_data >> trigger_dag_goals