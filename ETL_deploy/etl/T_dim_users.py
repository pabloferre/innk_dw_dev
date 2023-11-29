# Load libraries
import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import boto3
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn, categorize, execute_sql

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

##### Dependencies: E_dim_users.py  ###############


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

################################## AUXILIARY FUNCTIONS #################################

def convert_to_datetime(df: pd.DataFrame, col_names: list) -> pd.DataFrame:
    for column_name in col_names:
        try:
            # Convert to datetime and replace NaT with None
            df[column_name] = pd.to_datetime(df[column_name], unit='ms', errors='coerce').dt.date
            df[column_name] = df[column_name].where(df[column_name].notnull(), None)
            print(f"Converted {column_name} to datetime format without time.")
        except Exception as e:
            print(f"Failed to convert {column_name} to datetime format. Error: {e}")
            exit()
    return df

################################## MAIN FUNCTION #######################################

def main(url):
    
    if url == 'No new users to upload':
        
        print('No new users to upload')
        sys.stdout.write('No new users to upload')
        return 'No new users to upload'
    
    s3_client = boto3.client('s3',
                        aws_access_key_id=aws_access_id,
                        aws_secret_access_key=aws_access_key)

    
    s3_file_name =  url.split('.com/')[-1]
    
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df_users = pd.read_json(response.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        raise Exception(f"Error loading data from S3 - {status}")
    
    
    if len(df_users) == 0:
        print("No new users to upload")
        return None
    
    #Create dictionary from DIM_COMPANY table
    conn1 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    query1 = "Select id, company_db_id from public.dim_company"
    result1 = execute_sql(query1, conn1)
    comp_dic = pd.DataFrame(result1, columns=['id', 'company_db_id']).set_index('company_db_id')['id'].to_dict()   

    final_columns = ['company_id', 'user_db_id', 'name', 'last_name', 'email', 'position', 
                                       'contract_profile', 'area', 'sub_area', 'country',
                                       'created_at', 'updated_at', 'valid_from', 'valid_to', 'is_current']
    
    df_users['country'] = ''
    df_users['company_id'] = df_users['company_db_id'].apply(lambda x: categorize(x, comp_dic))
    df_users['valid_from'] = today
    df_users['valid_to'] = '9999-12-31'
    df_users['is_current'] = True
    df_users['company_id'] = df_users['company_id'].replace('None', None)
    df_users = df_users[final_columns]
    df_stage = convert_to_datetime(df_users, ['created_at', 'updated_at'])

    
    stage_file = df_stage.to_parquet()
    
    s3_file_name_stg = 'stage/' + str(today) + '_users.parquet'
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name_stg, Body=stage_file)
    
    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name_stg}'
    
    print(url)
    
    sys.stdout.write(url)
    
    return url


if __name__ == '__main__':
    url = sys.argv[1]
    main(url)
    
