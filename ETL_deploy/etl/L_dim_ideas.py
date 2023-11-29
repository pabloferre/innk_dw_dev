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
from lib.utils import get_conn, insert_batch, check_existing_records_ideas
today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")


##### Dependencies:  E_ideas_form_field_answers.py > T_param_class_table.py > L_param_class_table.py > T_ideas.py ###############

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




insert_msg = """INSERT INTO public.dim_idea (idea_db_id, tag_name, tag_description, tag_type,
    is_private, category, stage, like_ideas_count, average_general, name, description,
    problem_1, solution_1, problem_2, solution_2, problem_3, solution_3, problem_4, solution_4, problem_5, solution_5,
    problem_6, solution_6, name_embedded, prob_1_embedded, sol_1_embedded, prob_2_embedded, sol_2_embedded,
    prob_3_embedded, sol_3_embedded, prob_4_embedded,
    sol_4_embedded, prob_5_embedded, sol_5_embedded,
    prob_6_embedded, sol_6_embedded, created_at,
    updated_at, valid_from, valid_to, is_current) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
################################# MAIN FUNCTION ########################################


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
        
    df = check_existing_records_ideas(df, conn)

    print(df)
    
    insert_batch(df, conn, insert_msg, 100)

    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    return None
    
if __name__=='__main__':
    url = sys.argv[1].split(' ')[0]
    main(url)