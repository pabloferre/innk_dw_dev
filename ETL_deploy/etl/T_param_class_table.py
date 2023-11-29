# Load libraries
import os
import sys
import re
import traceback
import time
import pandas as pd
import numpy as np
from datetime import datetime
import boto3
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils_openai import classify_field

today = datetime.today().strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")



##### Dependencies:  E_ideas_form_field_answers.py  ###############


################################## ENVIRONMENT VARIABLES ################################
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

def categorize(df:pd.DataFrame)->pd.DataFrame:
    """Enumerates the fields in the classification table that are repeated for the same company
    
    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    for cat in ['Problem', 'Solution']:
        counter_dict = {}
        for i in df.index:
            if df.loc[i, 'category'] == cat:
                company_id = df.loc[i, 'company_id']
                counter_dict[company_id] = counter_dict.get(company_id, 0) + 1
                df.loc[i, 'category'] = f'{cat} {counter_dict[company_id]}'
    return df


def find_category(text):
    # Define the categories
    categories = ['Idea name', 'Problem', 'Solution', 'Other']

    # Search for each category in the text
    for category in categories:
        if re.search(re.escape(category), text):
            return category
    return text
    

################################## MAIN FUNCTION #####################################

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
        df = pd.read_json(response.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        raise Exception(f"Error loading data from S3 - {status}")
    
    df['category'] = ''
    
    df = df.drop_duplicates(subset=['field_id', 'company_id']).reset_index(drop=True)
    
    for i, row in df.iterrows():
        try:
            classification = classify_field(row['title'], row['description'])
            df.loc[i, 'category'] = find_category(classification)
            time.sleep(1)
        except Exception as e:
            print(f'Error in row {i}: {e}')
            traceback.print_exc()
            continue
    df = categorize(df)
    
    df.rename(columns={'title': 'name'}, inplace=True)
    
    df_stage = df[['company_id', 'field_id', 'name', 'description', 'category']]
    
    stage_file = df_stage.to_parquet()
    
    s3_file_name_stg = 'stage/' + str(today) + '_param_class_table.parquet'
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name_stg, Body=stage_file)
    
    
    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name_stg}'
    
    print(url)
    
    sys.stdout.write(url)
    
    return url


if __name__=='__main__':
    url = sys.argv[1]
    main(url)