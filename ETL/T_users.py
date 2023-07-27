# Load libraries
import os
import sys
import traceback
import time
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
from lib.general_module import get_conn, categorize

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')



######################## AUXILIARY DICTIONARIES #########################################
conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, company_db_id from innk_dw_dev.public.dim_company"

with conn.cursor() as cur:
    cur.execute(query)
    result = cur.fetchall()
conn.close()

comp_dic = pd.DataFrame(result, columns=['d', 'company_db_id']).set_index('company_db_id')['d'].to_dict() 

################################# COLUMNS ########################################################
users_columns = ['users', 'company_id', 'idea_db_id',  'user_db_id_1','user_name_1', 'user_last_name_1',
                 'user_email_1', 'position_1', 'contract_profile_1', 'area_1', 'sub_area_1','country_1',
                 'user_db_id_2','user_name_2', 'user_last_name_2', 'user_email_2', 'position_2',
                 'contract_profile_2', 'area_2', 'sub_area_2','country_2', 'user_db_id_3','user_name_3',
                 'user_last_name_3', 'user_email_3', 'position_3', 'contract_profile_3', 'area_3',
                 'sub_area_3','country_3', 'user_db_id_4','user_name_4', 'user_last_name_4',
                 'user_email_4', 'position_4', 'contract_profile_4', 'area_4', 'sub_area_4','country_4',
                 'valid_from', 'valid_to', 'is_current']

users_json_columns = ['user_db_id', 'company_id', 'email', 'name', 'last_name', 'position', 'contract_profile',
                      'area', 'sub_area', 'country', 'created_at', 'updated_at', 'valid_from', 'valid_to', 'is_current']


############################FUNCTIONS#####################################################  

def fill_users(df_group):
    # Order by user_db_id to ensure consistency
    df_group = df_group.sort_values('user_db_id')
    for i in range(4):
        i = i + 1
        df_group.loc[:, f'user_db_id_{i+1}'] = ''
        df_group.loc[:, f'user_name_{i+1}'] = ''
        df_group.loc[:, f'user_last_name_{i+1}'] = ''
        df_group.loc[:, f'user_email_{i+1}'] = ''
        df_group.loc[:, f'position_{i+1}'] = ''
        df_group.loc[:, f'contract_profile_{i+1}'] = ''
        df_group.loc[:, f'area_{i+1}'] = ''
        df_group.loc[:, f'sub_area_{i+1}'] = ''
        df_group.loc[:, f'country_{i+1}'] = ''
    # Create a list to store user details in JSON format
    json_users = []
    # Iterate over the users in the group
    for i, (_, user) in enumerate(df_group.iterrows()):
        if i <= 4:
            # For the first 4 users, fill the corresponding columns
            df_group.loc[:, f'user_db_id_{i+1}'] = user['user_db_id']
            df_group.loc[:, f'user_name_{i+1}'] = user['name']
            df_group.loc[:, f'user_last_name_{i+1}'] = user['last_name']
            df_group.loc[:, f'user_email_{i+1}'] = user['email']
            df_group.loc[:, f'position_{i+1}'] = user['position']
            df_group.loc[:, f'contract_profile_{i+1}'] = user['contract_profile']
            df_group.loc[:, f'area_{i+1}'] = user['area']
            df_group.loc[:, f'sub_area_{i+1}'] = user['sub_area']
            df_group.loc[:, f'country_{i+1}'] = user['country']
            df_group.loc[:, f'created_at'] = user['created_at']
            df_group.loc[:, f'updated_at'] = user['updated_at']

        # Add user information to json_users list irrespective of the user index
        json_users.append(user.to_json())
        
    # Assign json_users list to 'users' column
    df_group['users'] = [json_users] * len(df_group)

    return df_group


####################################################################################################


users = pd.read_json(r'H:\Mi unidad\Innk\users.json') #CAMBIAR EN ETL FINAL
ideas = pd.read_json(r'H:\Mi unidad\Innk\ideas_table.json')
ideas = ideas.loc[ideas['company_id'].isin(comp_dic.keys())]
users = users.loc[users['company_id'].isin(comp_dic.keys())]
users.rename(columns={'id':'user_db_id', 'first_name':'name'}, inplace=True)
#users['users']  = users.apply(lambda row: row.to_json(), axis=1)
users.loc[:,'company_id'] = users.loc[:, 'company_id'].apply(lambda x: categorize(int(x), comp_dic))
ideas.rename(columns={'id':'idea_db_id'}, inplace=True)

users['created_at'] = users['created_at'].dt.tz_localize(None)
users['updated_at'] = users['updated_at'].dt.tz_localize(None)
users['sub_area'] = ''
users['country'] = ''

df_merged = pd.merge(users, ideas[['user_id', 'idea_db_id']], left_on='user_db_id', right_on='user_id', how='left')
df_merged.drop_duplicates(subset=['idea_db_id', 'user_db_id'], inplace=True)
df_merged[['user_id', 'idea_db_id', 'area_id']] = df_merged[['user_id', 'idea_db_id', 'area_id']].astype('Int64')



df_final_users = df_merged.groupby('idea_db_id').apply(fill_users) 
df_final_users['valid_from'] = today
df_final_users['valid_to'] = '9999-12-31'
df_final_users['is_current'] = True
df_final_users['valid_from'] = df_final_users['valid_from'].dt.tz_localize(None)
df_final_users = df_final_users[users_columns]
