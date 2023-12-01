from dotenv import load_dotenv
import os
import pandas as pd
import boto3
import sys
from datetime import datetime
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn, categorize, execute_sql

load_dotenv()
bucket_name = os.environ.get('bucket_name')
aws_access_id = os.environ.get('aws_access_id')
aws_access_key = os.environ.get('aws_access_key')
aws_host = os.environ.get('aws_host')
aws_db_dw = os.environ.get('aws_db_dw')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')

today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

################################Dependecies (ETL dim/fact ideas) > get_clusters.py > E_param_clustered_ideas.py #######################

#################################AUXILIARY DICTIONARIES################################################################################



#Create dictionary from DIM_IDEAS table

conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, idea_db_id from public.dim_idea"
result = execute_sql(query, conn)
idea_dic = pd.DataFrame(result, columns=['id', 'idea_db_id']).set_index('idea_db_id')['id'].to_dict()


############################################# MAIN FUNCTION ##########################################################################

def main(url):

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
    
    df['idea_id'] = df['idea_db_id'].apply(lambda x: categorize(x, idea_dic))
    df.rename(columns={'idea_name':'idea_c_name', 'idea_description':'idea_c_description', 'cluster':'cluster_number'}, inplace=True)
    df = df[['idea_id', 'idea_db_id', 'company_id', 'goal_id','idea_c_name', 'idea_c_description', 'cluster_name', 
            'cluster_description', 'cluster_number', 'distance_centroid']]
    
    stage_file = df.to_parquet()
    
    s3_file_name_stg = 'stage/' + str(today) + 'param_clustered_ideas.parquet'
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name_stg, Body=stage_file)
    
    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name_stg}'
    
    print(url)
    
    sys.stdout.write(url)
    
    return url
    


if __name__ == '__main__':
    url = sys.argv[1]
    main(url)
