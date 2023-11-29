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


def get_users(conn, conn2)->pd.DataFrame: 
    
    '''This function returns a dataframe with  the goals in the database'''
    
    q1 = f""" select company_id as "company_db_id", users.id as "user_db_id", first_name as "name", last_name,
    email, position, contract_profile, area_id as "area", company_area_detail_id as "sub_area", created_at, updated_at 
    from public.users;"""
    q2= f"""select user_db_id from dim_users;"""
    
    with conn.cursor() as cur:
        cur.execute(q1)
        result = cur.fetchall()
    df = pd.DataFrame(result, columns=['company_db_id', 'user_db_id', 'name', 'last_name', 'email', 'position', 
                                       'contract_profile', 'area', 'sub_area', 'created_at', 'updated_at'])
    
    with conn2.cursor() as cur2:
        cur2.execute(q2)
        result2 = cur2.fetchall()    
    df2 = pd.DataFrame(result2, columns=['user_db_id'])
    
    df_final = df[~df['user_db_id'].isin(df2['user_db_id'])]
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
    df = get_users(conn, conn2)
    # Define the bucket name and S3 file name
    
    if df.empty:
        
        print('No new users to upload')
        sys.stdout.write('No new users to upload')
        return 'No new users to upload'

    s3_file_name = 'raw/' + str(today) + '_users.json'

    # Upload the JSON string to S3
    json_file = df.to_json(orient='records')
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=json_file)

    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name}'


    print(url)
    
    sys.stdout.write(url)
    
    return url

if __name__ == '__main__':
    main()