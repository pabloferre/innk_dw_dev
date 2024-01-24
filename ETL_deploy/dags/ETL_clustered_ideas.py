from airflow.models import DAG 
from airflow.operators.bash import BashOperator
import subprocess
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 18),
    'retries': 5,
}

with DAG(
    'ETL_clustered_ideas',
    default_args=default_args,
    description='ETL for param clustered ideas table',
    catchup = False
    ) as dag:

    #Get clusterization

    get_clusters = BashOperator(
        task_id='get_clusters',
        bash_command=f'python3 /usr/local/airflow/cluster/app/get_clusters.py',
        do_xcom_push=True,
        dag=dag,
    )

    #Extract data

    extract_data = BashOperator(
        task_id='extract_data',
        bash_command='python3 /usr/local/airflow/etl/E_param_clustered_ideas.py "{{ ti.xcom_pull(task_ids=\'get_clusters\') }}"',
        do_xcom_push=True,
        dag=dag,
    )


    #Transform
    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='python3 /usr/local/airflow/etl/T_param_clustered_ideas.py "{{ ti.xcom_pull(task_ids=\'extract_data\') }}"',
        do_xcom_push=True,
        dag=dag
    )

    #Load

    load_data = BashOperator(
        task_id='load_data',
        bash_command='python3 /usr/local/airflow/etl/L_param_clustered_ideas.py "{{ ti.xcom_pull(task_ids=\'transform_data\') }}"',
        dag=dag
    )



get_clusters >> extract_data  >> transform_data >> load_data
