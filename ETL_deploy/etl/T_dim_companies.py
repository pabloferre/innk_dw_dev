# Load libraries
import os
import sys
import pandas as pd
from datetime import datetime
import boto3
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)

today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")


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

def convert_to_datetime(df: pd.DataFrame, col_names: list) -> pd.DataFrame:
    for column_name in col_names:
        try:
            # Convert to datetime and replace NaT with None
            df[column_name] = pd.to_datetime(df[column_name], errors='coerce').dt.date
            df[column_name] = df[column_name].where(df[column_name].notnull(), None)
            print(f"Converted {column_name} to datetime format without time.")
        except Exception as e:
            print(f"Failed to convert {column_name} to datetime format. Error: {e}")
            exit()
    return df
################################## MAIN FUNCTION #######################################


def main(url):
    
    if url == 'No new companies to upload':
        
        print('No new companies to upload')
        sys.stdout.write('No new companies to upload')
        return 'No new companies to upload'
    
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_id,
                            aws_secret_access_key=aws_access_key)

    
    s3_file_name =  url.split('.com/')[-1]
    
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_json(response.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        raise Exception(f"Error loading data from S3 - {status}")
    
    df_stage = convert_to_datetime(df, ['created_at', 'updated_at'])
    df_stage['valid_from'] = today
    df_stage['valid_to'] = '9999-12-31'
    df_stage['is_current'] = True
    
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