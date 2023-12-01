# Load libraries
import os
import sys
import pandas as pd
from datetime import datetime
import boto3
from dotenv import load_dotenv
from io import BytesIO
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn

today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

###########################  Dependencies: E_companies.py > T_companies.py  ###############


################################## ENVIRONMENT VARIABLES ###############################
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db_dw = os.environ.get('aws_db_dw')
aws_access_id = os.environ.get('aws_access_id')
aws_access_key = os.environ.get('aws_access_key')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
bucket_name = os.environ.get('bucket_name')


################################## AUXILIARY FUNCTIONS #################################

def insert_data(df, conn):
    """Insert data into goals table in database"""

    insert = """insert into public.dim_goals 
        (goal_db_id, company_id, goal_name, goal_description, active,
            ideas_reception, is_private, end_campaign, created_at, updated_at, 
            valid_from, valid_to, is_current)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

    data = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        # Execute the batch insert
        cur.executemany(insert, data)
        # Commit the changes
        conn.commit()
    conn.close()
    return None

################################# MAIN FUNCTION ########################################


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
        # Use BytesIO buffer to read the Parquet data
        data_buffer = BytesIO(response['Body'].read())
        df = pd.read_parquet(data_buffer)

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        raise Exception(f"Error loading data from S3 - {status}")
        
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    
    insert_data(df, conn)
    
    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    print('Data loaded successfully.')
    return None

if __name__=='__main__':
    url = sys.argv[1]
    main(url)