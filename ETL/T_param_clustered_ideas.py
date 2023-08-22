from dotenv import load_dotenv
import os
import pandas as pd
import time
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
from lib.general_module import get_conn, categorize, execute_sql

load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
path_to_drive = os.environ.get('path_to_drive')

#################################AUXILIARY DICTIONARIES#################################################

#Create dictionary from DIM_COMPANY table
conn1 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query1 = "Select id, company_db_id from innk_dw_dev.public.dim_company"
result1 = execute_sql(query1, conn1)
comp_dic = pd.DataFrame(result1, columns=['id', 'company_db_id']).set_index('company_db_id')['id'].to_dict()   


#Create dictionary from DIM_IDEAS table

conn2 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query2 = "Select id, idea_db_id from innk_dw_dev.public.dim_idea"
result2 = execute_sql(query2, conn2)
idea_dic = pd.DataFrame(result2, columns=['id', 'idea_db_id']).set_index('idea_db_id')['id'].to_dict()

#Create dictionary from DIM_GOAL table
conn3 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query3 = "Select id, goal_db_id from innk_dw_dev.public.dim_goal"
result3 = execute_sql(query3, conn3)
goal_dic = pd.DataFrame(result3, columns=['id', 'goal_db_id']).set_index('goal_db_id')['id'].to_dict()
########################################################################################################



df = pd.read_excel(path_to_drive + r'clustered_processed_117.xlsx')
df_ideas = pd.read_json(path_to_drive + r'raw/ideas_table.json')
df = df.merge(df_ideas[['id', 'goal_id', 'company_id']], how='left', left_on='idea_db_id', right_on='id')

df['goal_id'] = df['goal_id'].apply(lambda x: categorize(x, goal_dic))
df['company_id'] = df['company_id'].apply(lambda x: categorize(x, comp_dic))
df['idea_id'] = df['idea_db_id'].apply(lambda x: categorize(x, idea_dic))
df.rename(columns={'name':'idea_c_name', 'description':'idea_c_description', 'cluster':'cluster_number'}, inplace=True)
df = df[['idea_id', 'idea_db_id', 'company_id', 'goal_id','idea_c_name', 'idea_c_description', 'cluster_name', 
         'cluster_description', 'cluster_number']]
df.to_parquet(path_to_drive + r'stage/param_clustered_ideas_117.parquet', index=False)