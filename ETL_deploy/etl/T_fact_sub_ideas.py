# Load libraries
import os
import sys
import traceback
import json
import numpy as np
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn, categorize, execute_sql

today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
default_none_date = datetime.strptime('2260-12-31', "%Y-%m-%d")

##### THIS SCRIPT USES DATA GENERATED FROM T_ideas.py .#######################

##### Dependencies: E_ideas_form_field_answers.py > T_param_class_table.py > L_param_class_table.py > T_ideas.py ###############



###############################ENVIRONMENT VARIABLES#####################################
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_access_id = os.environ.get('aws_access_id')
aws_access_key = os.environ.get('aws_access_key')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
bucket_name = os.environ.get('bucket_name')

######################## AUXILIARY DICTIONARIES #########################################


#Create dictionary from DIM_USERS table
conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, user_db_id from public.dim_users"
result = execute_sql(query, conn)
user_dic = pd.DataFrame(result, columns=['id', 'user_db_id']).set_index('user_db_id')['id'].to_dict() 

conn1 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query1 = "Select id, idea_db_id from public.dim_idea"
result1 = execute_sql(query1, conn1)
idea_dic = pd.DataFrame(result1, columns=['id', 'idea_db_id']).set_index('idea_db_id')['id'].to_dict()




################################# MAIN FUNCTION ########################################



def main(url):
    
    if url == 'No new data to load.':
        print('No new data to load.')
        sys.stdout.write('No new data to load.')
        return 'No new data to load.'
    
    s3_client = boto3.client('s3',
                aws_access_key_id=aws_access_id,
                aws_secret_access_key=aws_access_key)

    
    s3_file_name =  url.split('.com/')[-1]
    
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        # Use BytesIO buffer to read the Parquet data
        data_buffer = BytesIO(response['Body'].read())
        df_fact_sub_idea = pd.read_parquet(data_buffer)

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        raise Exception(f"Error loading data from S3 - {status}")
        
    
    #Get contributions table to fill users columns
    q1 = f"""SELECT idea_id, user_id
    From public.contributions"""
    conn_ = get_conn(aws_host, aws_db, aws_port, aws_user_db, aws_pass_db)
    result_ = execute_sql(q1, conn_)
    df_collaborations = pd.DataFrame(result_, columns=['idea_id', 'user_id'])
    

    # sort the data frame if necessary, this is to ensure the order of user_id's for each idea_id
    df_collaborations.sort_values(by=['idea_id'], inplace=True)

    df_collaborations['user_id'] = df_collaborations['user_id'].apply(lambda x: categorize(x, user_dic))

    # create a new column with count of users for each idea
    df_collaborations['user_num'] = df_collaborations.groupby('idea_id').cumcount() + 1

    # pivot the data frame
    df_pivot = df_collaborations.pivot(index='idea_id', columns='user_num', values='user_id')

    # rename the columns
    df_pivot.columns = [f'user_id_{i}' for i in df_pivot.columns]

    # reset the index to make idea_id a column again
    df_pivot.reset_index(inplace=True)

    # Define the columns to split the DataFrame
    cols_first_part = ['idea_id', 'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4']
    cols_second_part = ['idea_id', 'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4'] +\
        [col for col in df_pivot.columns if 'user_id' in col and col not in cols_first_part]

    # Split the DataFrame
    df_first_part = df_pivot[cols_first_part]
    df_second_part = df_pivot[cols_second_part]

    # Function to convert row into JSON
    def row_to_json(row):
        # Keep only non-null values
        user_ids = row.dropna().to_dict()
        user_ids.pop('idea_id', None)
        return json.dumps(user_ids)

    # Convert the second part into JSON
    df_second_part['users'] = df_second_part.apply(row_to_json, axis=1)

    # Keep only 'idea_id' and 'json_column' in the second part
    df_second_part = df_second_part[['idea_id', 'users']]

    # Merge the JSON data back to the first part
    df_final = pd.merge(df_first_part, df_second_part, on='idea_id', how='left')


    df_final_fact_sub_idea = pd.merge(df_fact_sub_idea, df_final, left_on='idea_db_id', right_on='idea_id', how='left')
    df_final_fact_sub_idea['idea_id'] = df_final_fact_sub_idea['idea_db_id'].apply(lambda x: categorize(x, idea_dic))
    df_final_fact_sub_idea = df_final_fact_sub_idea[['idea_id', 'company_id', 'user_id_1', 'user_id_2', 'user_id_3',
                                                    'user_id_4', 'users', 'goal_id', 'submited_at']]
    
    user_cols =['company_id', 'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4', 'goal_id']
    df_final_fact_sub_idea[user_cols] = df_final_fact_sub_idea[user_cols].replace(np.nan, 0)
    df_final_fact_sub_idea[user_cols] = df_final_fact_sub_idea[user_cols].replace('None', 0)
    df_final_fact_sub_idea[['company_id', 'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4', 'goal_id']] = df_final_fact_sub_idea[['company_id',
                                                                                            'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4', 'goal_id']].astype(int)
    df_final_fact_sub_idea['submited_at'] = df_fact_sub_idea['submited_at'].replace({None: '1900-01-01 00:00:00'})
     
    df_final_fact_sub_idea = df_final_fact_sub_idea.loc[~(df_final_fact_sub_idea.loc[:, 'idea_id'].isin(['None', None]))]
    
    #Upload to S3

    stage_file_fact = df_final_fact_sub_idea.to_parquet()
    
    s3_file_name_stg_fact = 'stage/' + str(today) + '_FACT_submitted_ideas.parquet'
    
    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name_stg_fact, Body=stage_file_fact)
    
    url_fact = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name_stg_fact}'
    
    print(url_fact)
    
    sys.stdout.write(url_fact)
    
    return url_fact

if __name__ == '__main__':
    url = sys.argv[1].split(' ')[1]
    main(url)