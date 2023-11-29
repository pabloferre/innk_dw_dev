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

##### Dependencies: E_goal.py  ###############


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
    
    if url == 'No new goals to upload':
        
        print('No new goals to upload')
        sys.stdout.write('No new goals to upload')
        return 'No new goals to upload'
    
    s3_client = boto3.client('s3',
                        aws_access_key_id=aws_access_id,
                        aws_secret_access_key=aws_access_key)

    
    s3_file_name =  url.split('.com/')[-1]
    
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df_goal = pd.read_json(response.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        raise Exception(f"Error loading data from S3 - {status}")
    
    #Create dictionary from DIM_COMPANY table
    conn1 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    query1 = "Select id, company_db_id from public.dim_company"
    result1 = execute_sql(query1, conn1)
    comp_dic = pd.DataFrame(result1, columns=['id', 'company_db_id']).set_index('company_db_id')['id'].to_dict()   

    final_columns = ['goal_db_id', 'company_id', 'goal_name', 'goal_description', 'active',
                 'ideas_reception', 'is_private', 'end_campaign', 'created_at', 'updated_at', 
                 'valid_from', 'valid_to', 'is_current']


    df_goal.rename(columns={'id': 'goal_db_id', 'name': 'goal_name', 'description': 'goal_description'}, inplace=True)
    df_goal['company_id'] = df_goal['company_id'].apply(lambda x: categorize(x, comp_dic))
    df_goal['valid_from'] = today
    df_goal['valid_to'] = '9999-12-31'
    df_goal['is_current'] = True
    df_goal['company_id'] = df_goal['company_id'].replace('None', None)
    df_goal = df_goal[final_columns]
    df_stage = convert_to_datetime(df_goal, ['end_campaign', 'created_at', 'updated_at'])
    
    stage_file = df_stage.to_parquet()
    
    s3_file_name_stg = 'stage/' + str(today) + '_companies.parquet'
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name_stg, Body=stage_file)
    
    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name_stg}'
    
    print(url)
    
    sys.stdout.write(url)
    
    return url


if __name__ == '__main__':
    url = sys.argv[1]
    main(url)
    

