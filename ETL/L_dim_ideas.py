# Load libraries
import os
import sys
import traceback
import time
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
print(path)
os.chdir(path)
from lib.general_module import get_conn

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")


##### Dependencies: E_ideas_form_field_answers.py > T_ideas.py ###############


###############################ENVIRONMENT VARIABLES#####################################
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
path_to_drive = os.environ.get('path_to_drive')


######################## AUXILIARY FUNCTIONS #########################################


def check_existing_records(df, conn):
    """Check if records in the DataFrame already exist in the database"""
    existing_ids = set()

    # Fetch existing idea_db_ids from the database
    with conn.cursor() as cur:
        cur.execute("SELECT idea_db_id FROM innk_dw_dev.public.dim_idea")
        existing_ids.update(row[0] for row in cur.fetchall())

    # Filter out existing records from the DataFrame
    new_records = df[~df['idea_db_id'].isin(existing_ids)]
    return new_records

def insert_data(df, conn):
    """Insert data into ideas table in database"""

    insert = """INSERT INTO innk_dw_dev.public.dim_idea (idea_db_id, tag_name, tag_description, tag_type,
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
    data = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        # Execute the batch insert
        cur.executemany(insert, data)
        # Commit the changes
        conn.commit()
    conn.close()
    return None



def main():
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    dim_idea = pd.read_parquet(path_to_drive + 'stage/dim_idea.parquet')
    dim_idea = check_existing_records(dim_idea, conn)
    #dim_idea.drop(columns=['company_id', 'user_id'], inplace=True)
    insert_data(dim_idea, conn)
    
if __name__=='__main__':
    main() 