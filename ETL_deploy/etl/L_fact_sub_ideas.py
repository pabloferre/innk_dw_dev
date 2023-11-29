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
from lib.utils import get_conn, insert_batch

today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
default_none_date = datetime.strptime('2260-12-31', "%Y-%m-%d")


##### Dependencies:  E_ideas_form_field_answers.py > T_param_class_table.py > L_param_class_table.py > T_fact_sub_idea.py ###############

################################## ENVIRONMENT VARIABLES ###############################
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

insert_msg = """INSERT INTO public.fact_submitted_idea (idea_id, company_id, user_id_1, \
    user_id_2, user_id_3, user_id_4, users, goal_id, submitted_at) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """


def main(url):
    
    if url == 'No new data to load.':
        print('No new data to load.')
        sys.stdout.write('No new data to load.')
        return 'No new data to load.'
    
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    
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
        
    print(len(df))
    print(df)
    
    
    insert_batch(df, conn, insert_msg, 100)
    
    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    return None

if __name__=='__main__':
    url = sys.argv[1]    
    main(url) 