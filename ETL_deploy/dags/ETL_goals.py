from airflow.models import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2010, 1, 1, 0, 0, 0),
    'schedule_interval': None,
    'retries': 5,
    'max_active_runs': 1,
}


with DAG(
    'ETL_goals',
    default_args=default_args,
    description='ETL for goals table',
    catchup = False
    ) as dag:

    #Extract

    extract_data = BashOperator(
        task_id='extract_data',
        bash_command=f'python3 /usr/local/airflow/etl/E_dim_goals.py',
        do_xcom_push=True,
        dag=dag,
    )

    #Transform
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='python3 /usr/local/airflow/etl/T_dim_goals.py "{{ ti.xcom_pull(task_ids=\'extract_data\') }}"',
        do_xcom_push=True,
        dag=dag
    )

    #Load

    load_data = BashOperator(
        task_id='load_data',
        bash_command='python3 /usr/local/airflow/etl/L_dim_goals.py "{{ ti.xcom_pull(task_ids=\'transform_data\') }}"',
        dag=dag
    )

    #Trigger dag ideas
    
    trigger_dag_ideas = TriggerDagRunOperator(
        task_id='trigger_dag_ideas',
        trigger_dag_id='ETL_ideas',
        wait_for_completion=True,
        dag=dag
    )


extract_data >> transform_data >> load_data >> trigger_dag_ideas