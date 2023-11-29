# Load libraries
import os
import sys
import pandas as pd
from datetime import datetime
import boto3
from dotenv import load_dotenv
import re
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn
today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")


################################## ENVIRONMENT VARIABLES ###############################
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_access_id = os.environ.get('aws_access_id')
aws_access_key = os.environ.get('aws_access_key')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
bucket_name = os.environ.get('bucket_name')

##################################AUXILIARY FUNCTIONS#################################

def remove_illegal_chars(val):
    if isinstance(val, str):
        # XML 1.0 legal characters: #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | 
        # [#x10000-#x10FFFF]
        # To simplify, we will keep: ( #x9 | #xA | #xD | [#x20-#x7E] ),
        # accented characters and usual printable characters
        val = re.sub(r'[^\x09\x0A\x0D\x20-\x7EÁÉÍÓÚáéíóú]', '', val)
    return val

def get_forms_with_answers(conn, comp: pd.DataFrame): #Mejorar la funcion sacando el loop con otra query
    
    '''This function returns a dataframe with the answers to the forms of the companies in the comp dataframe'''
    
    df_combined = pd.DataFrame()
    for i in comp['id']:
        print(i)
        c = i
        qy = f""" select ifa.idea_id as "idea_id", cff.id as "field_id",ifa.answer as "field_answer", cff.company_id as "company_id", 
                cff.title as "title", cff.description as "description", cff.form_field_id as "form_field_id", cff.form_id as "form_id",
                ideas.title as "idea_name", ideas.description as "idea_description"
                from idea_field_answers ifa 
                join company_form_fields cff on ifa.company_form_field_id = cff.id 
                join ideas on ideas.id = ifa.idea_id 
                where cff.company_id = {c};"""
        
        with conn.cursor() as cur:
            cur.execute(qy)
            result = cur.fetchall()
        temp = pd.DataFrame(result, columns=['idea_id', 'field_id', 'field_answer', 'company_id', 'title', 'description', 'form_field_id', 
                                            'form_id', 'idea_name', 'idea_description'])
        df_combined = pd.concat([df_combined, temp], ignore_index=True)
        
    conn.close()
    return df_combined


################################## MAIN FUNCTION ######################################

def main():
    
    s3_client = boto3.client('s3',
                             aws_access_key_id=aws_access_id,
                             aws_secret_access_key=aws_access_key)

    
    conn = get_conn(aws_host, aws_db, aws_port, aws_user_db, aws_pass_db)
    conn2 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    
    q1 = f""" select company_db_id as "id" from public.dim_company;"""
    
    with conn2.cursor() as cur:
        cur.execute(q1)
        result = cur.fetchall()
    comp = pd.DataFrame(result, columns=['id'])
    
    conn3 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    q2 = f""" select idea_db_id from public.dim_idea;"""
    
    with conn3.cursor() as cur:
        cur.execute(q2)
        result = cur.fetchall()
    df2 = pd.DataFrame(result, columns=['idea_db_id'])
    
    
    df_combined = get_forms_with_answers(conn, comp)
    
    #Filter already loaded data
    df_combined = df_combined[~df_combined['idea_id'].isin(df2['idea_db_id'])]
    df = df_combined.applymap(remove_illegal_chars)
    
    if df.empty:
        
        print('No new data to load.')
        sys.stdout.write('No new data to load.')
        return 'No new data to load.'
    
    
    # Define the bucket name and S3 file name

    s3_file_name = 'raw/' + str(today) + '_ideas_form_field_answer.json'

    # Upload the JSON string to S3
    json_file = df.to_json(orient='records')
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=json_file)

    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name}'


    print(url)
    
    sys.stdout.write(url)
    
    return url

if __name__ == '__main__':
    main()
