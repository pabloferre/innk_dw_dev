# Load libraries
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

############################# Dependencies: T_goal.py  ###############


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

def insert_data(df, conn):
    """Insert data into ideas table in database"""

    insert = """INSERT INTO innk_dw_dev.public.dim_goal (goal_db_id, company_id, goal_name, \
    goal_description, active, ideas_reception, is_private, end_campaign, created_at,
    updated_at, valid_from, valid_to, is_current) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s, %s)
    """
    data = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        # Execute the batch insert
        cur.executemany(insert, data)
        # Commit the changes
        conn.commit()
    conn.close()
    return None

################################# MAIN FUNCTION ########################################


def main(path):
    
    dim_goal = pd.read_parquet(path)
    print(dim_goal.tail())
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    insert_data(dim_goal, conn)
    return None

if __name__=='__main__':
    path = sys.argv[1]    
    main(path) 
