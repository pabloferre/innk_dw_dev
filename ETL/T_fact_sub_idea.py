# Load libraries
import os
import sys
import traceback
import json
import numpy as np
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.general_module import get_conn, categorize, execute_sql

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

##### THIS SCRIPT USES DATA GENERATED FROM T_ideas.py .#######################

##### Dependencies: E_ideas_form_field_answers.py > T_ideas.py ###############



###############################ENVIRONMENT VARIABLES#####################################
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
path_to_drive = os.environ.get('path_to_drive')


######################## AUXILIARY DICTIONARIES #########################################


#Create dictionary from DIM_USERS table
conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, user_db_id from innk_dw_dev.public.dim_users"
result = execute_sql(query, conn)
user_dic = pd.DataFrame(result, columns=['id', 'user_db_id']).set_index('user_db_id')['id'].to_dict() 

conn1 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query1 = "Select id, idea_db_id from innk_dw_dev.public.dim_idea"
result1 = execute_sql(query1, conn1)
idea_dic = pd.DataFrame(result1, columns=['id', 'idea_db_id']).set_index('idea_db_id')['id'].to_dict()

#########################################################################################






def main():
    df_collaborations = pd.read_json(path_to_drive + r'raw/collaborations.json')
    df_fact_sub_idea = pd.read_parquet(path_to_drive + r'raw/fact_sub_idea.parquet')

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
    
    #df_final_fact_sub_idea['submited_at'] = df_final_fact_sub_idea['submited_at'].dt.tz_localize(None)
    user_cols =['company_id', 'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4', 'goal_id']
    df_final_fact_sub_idea[user_cols] = df_final_fact_sub_idea[user_cols].replace(np.nan, 0)
    df_final_fact_sub_idea[user_cols] = df_final_fact_sub_idea[user_cols].replace('None', 0)
    df_final_fact_sub_idea[['company_id', 'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4', 'goal_id']] = df_final_fact_sub_idea[['company_id',
                                                                                            'user_id_1', 'user_id_2', 'user_id_3', 'user_id_4', 'goal_id']].astype(int)
    df_final_fact_sub_idea['submited_at'] = df_fact_sub_idea['submited_at'].replace({None: '1900-01-01 00:00:00'})
    df_final_fact_sub_idea.to_parquet(path_to_drive + 'stage/fact_sub_idea.parquet', index=False)
    
    return None

if __name__ == '__main__':
    main()