# Load libraries
import os
import sys
import pandas as pd
from datetime import datetime
import boto3
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn

today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")


################################## ENVIRONMENT VARIABLES ################################
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


def get_companies(conn, conn2)->pd.DataFrame: 
    
    '''This function returns a dataframe with  the companies in the database'''
    
    q1 = f""" select companies.id as "company_db_id", name as "company_name", status as "status_active", created_at, updated_at
            from companies;"""
    q2= f""" select company_db_id from dim_company;"""
    
    with conn.cursor() as cur:
        cur.execute(q1)
        result = cur.fetchall()
    df = pd.DataFrame(result, columns=['company_db_id', 'company_name', 'status_active', 'created_at', 'updated_at'])
    
    with conn2.cursor() as cur2:
        cur2.execute(q2)
        result2 = cur2.fetchall()    
    df2 = pd.DataFrame(result2, columns=['company_db_id'])
    
    df_final = df[~df['company_db_id'].isin(df2['company_db_id'])]
    conn.close()
    conn2.close()
    return df_final

################################## MAIN FUNCTION #####################################

def main():
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_id,
                             aws_secret_access_key=aws_access_key)


    conn = get_conn(aws_host, aws_db, aws_port, aws_user_db, aws_pass_db)
    conn2 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    df = get_companies(conn, conn2)
    
    if df.empty:
        print('No new companies to upload')
        sys.stdout.write('No new companies to upload')
        return 'No new companies to upload'
    
    # Define the bucket name and S3 file name

    s3_file_name = 'raw/' + str(today) + '_companies.json'

    # Upload the JSON string to S3
    json_file = df.to_json(orient='records')
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=json_file)

    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name}'


    print(url)
    
    sys.stdout.write(url)
    
    return url

if __name__ == '__main__':
    main()