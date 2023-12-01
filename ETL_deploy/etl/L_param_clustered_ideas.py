from dotenv import load_dotenv
import os
import pandas as pd
import boto3
import sys
from datetime import datetime
from io import BytesIO
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn

today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

###############################ENVIRONMENT VARIABLES#####################################
load_dotenv()
bucket_name = os.environ.get('bucket_name')
aws_access_id = os.environ.get('aws_access_id')
aws_access_key = os.environ.get('aws_access_key')
aws_host = os.environ.get('aws_host')
aws_db_dw = os.environ.get('aws_db_dw')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')



###################Dependecies (ETL dim/fact ideas) > get_clusters.py > E_param_clustered_ideas.py>T_param_clustered_ideas.py #######################

######################## AUXILIARY FUNCTIONS #############################################################################

def insert_data(df, conn):
    """Insert data into ideas table in database"""

    insert = """INSERT INTO public.param_clustered_ideas (idea_id, idea_db_id, company_id, \
    goal_id, idea_c_name, idea_c_description, cluster_name, cluster_description, cluster_number, distance_centroid) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    data = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        # Execute the batch insert
        cur.executemany(insert, data)
        # Commit the changes
        conn.commit()
    conn.close()
    return None

def check_existing_records_ideas(df, conn):
    """Check if records in the DataFrame already exist in the database"""
    existing_ids = set()

    # Fetch existing idea_db_ids from the database
    with conn.cursor() as cur:
        cur.execute("SELECT idea_db_id FROM public.param_clustered_ideas")
        existing_ids.update(row[0] for row in cur.fetchall())

    # Filter out existing records from the DataFrame
    new_records = df[~df['idea_db_id'].isin(existing_ids)]
    return new_records


############################### MAIN FUNCTION ##########################################################################

def main(url):
    
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

    insert_data(df, conn)
    
    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    return None


if __name__=='__main__':
    url = sys.argv[1]    
    main(url) 