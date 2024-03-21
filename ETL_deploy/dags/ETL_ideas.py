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
    'ETL_ideas',
    default_args=default_args,
    description='ETL for dim_ideas, fact_ideas and param_class_table',
    catchup = False
    ) as dag:

    #Extract
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command=f'python3 /usr/local/airflow/etl/E_ideas_form_field_answers.py',
        do_xcom_push=True,
        dag=dag,
    )

    #Transform param_class_table
    transform_param_class_table = BashOperator(
        task_id='T_param_class_table',
        bash_command='python3 /usr/local/airflow/etl/T_param_class_table.py "{{ ti.xcom_pull(task_ids=\'extract_data\') }}"',
        do_xcom_push=True,
        dag=dag
    )

    #Load param_class_rable
    load_param_class_table = BashOperator(
        task_id='L_param_class_table',
        bash_command='python3 /usr/local/airflow/etl/L_param_class_table.py "{{ ti.xcom_pull(task_ids=\'T_param_class_table\') }}"',
        dag=dag
    )

    #Transform ideas table and create Dim_ideas and Fact_subimitted_ideas tables
    transform_ideas = BashOperator(
        task_id='T_ideas',
        bash_command='python3 /usr/local/airflow/etl/T_ideas.py "{{ ti.xcom_pull(task_ids=\'extract_data\') }}"',
        do_xcom_push=True,
        dag=dag
    )

    transform_fact_sub_ideas = BashOperator(
        task_id='T_fact_sub_ideas',
        bash_command='python3 /usr/local/airflow/etl/T_fact_sub_ideas.py "{{ ti.xcom_pull(task_ids=\'T_ideas\') }}"',
        do_xcom_push=True,
        dag=dag
    )


    #Load Dim and Fact ideas tables
    load_dim_ideas = BashOperator(
        task_id='L_dim_ideas',
        bash_command='python3 /usr/local/airflow/etl/L_dim_ideas.py "{{ ti.xcom_pull(task_ids=\'T_ideas\') }}"',
        dag=dag
    )

    load_fact_sub_ideas = BashOperator(
        task_id='L_fact_sub_ideas',
        bash_command='python3 /usr/local/airflow/etl/L_fact_sub_ideas.py "{{ ti.xcom_pull(task_ids=\'T_fact_sub_ideas\') }}"',
        dag=dag
    )

    #Load vector to Pinecone
    load_emb_ideas = BashOperator(
        task_id='L_emb_ideas',
        bash_command='python3 /usr/local/airflow/etl/L_idea_emb_pinecone.py',
        dag=dag
    )
    
    #Trigger dag clustered_ideas
    trigger_dag_clustered_ideas = TriggerDagRunOperator(
        task_id='trigger_dag_clustered_ideas',
        trigger_dag_id='ETL_clustered_ideas',
        wait_for_completion=True,
        dag=dag
    )

extract_data >> transform_param_class_table >> load_param_class_table >> transform_ideas >> load_dim_ideas >> transform_fact_sub_ideas >> load_fact_sub_ideas >> load_emb_ideas >> trigger_dag_clustered_ideas