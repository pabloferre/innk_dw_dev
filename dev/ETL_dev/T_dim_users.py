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
path_to_drive = os.environ.get('path_to_drive')



######################## AUXILIARY DICTIONARIES #########################################
conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, company_db_id from innk_dw_dev.public.dim_company"

with conn.cursor() as cur:
    cur.execute(query)
    result = cur.fetchall()
conn.close()

comp_dic = pd.DataFrame(result, columns=['d', 'company_db_id']).set_index('company_db_id')['d'].to_dict() 

################################# COLUMNS ########################################################

users_columns = ['company_id', 'user_db_id', 'email', 'name', 'last_name', 'position', 'contract_profile',
                      'area', 'sub_area', 'country', 'created_at', 'updated_at', 'valid_from', 'valid_to', 'is_current']


############################FUNCTIONS#############################################################  



##################################################################################################

def main(path):

    users = pd.read_json(path) 

    users = users.loc[users['company_id'].isin(comp_dic.keys())]
    users.rename(columns={'id':'user_db_id', 'first_name':'name'}, inplace=True)
    users.loc[:,'company_id'] = users.loc[:, 'company_id'].apply(lambda x: categorize(int(x), comp_dic))

    users['created_at'] = users['created_at'].dt.tz_localize(None)
    users['updated_at'] = users['updated_at'].dt.tz_localize(None)
    users['sub_area'] = ''
    users['country'] = ''


    df_final_users = users
    df_final_users['valid_from'] = today
    df_final_users['valid_to'] = '9999-12-31'
    df_final_users['is_current'] = True
    df_final_users['valid_from'] = df_final_users['valid_from'].dt.tz_localize(None)
    df_final_users = df_final_users[users_columns]
    df_final_users.to_excel(path_to_drive + r'stage/dim_user.xlsx', index=False)
    
if __name__ == '__main__':
    path = sys.argv[1]
    main(path_to_drive + r'raw/users.json') #CAMBIAR EN ETL FINAL