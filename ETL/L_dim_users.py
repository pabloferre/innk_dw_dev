
# Load libraries
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
from lib.general_module import connect_db, get_conn

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
path_to_drive = os.environ.get('path_to_drive')
crdenc = {'dbname': aws_db_dw, 'user': aws_user_db, 'password': aws_pass_db, 'host': aws_host, 'port': aws_port}



def insert_data(df: pd.Dataframe, conn):
    """Insert data into ideas table in database"""

    insert = """INSERT INTO innk_dw_dev.public.dim_users (company_id, user_db_id, email, name, last_name, position, \
        contract_profile, area, sub_area, country, created_at, updated_at, valid_from, valid_to, is_current\
    ) \
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    data = list(df.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        # Execute the batch insert
        cur.executemany(insert, data)
        # Commit the changes
        conn.commit()
    conn.close()
    return None


def main(path): #CAMBIAR  EN ETL FINAL
    #engine = connect_db(crdenc)
    dim_users = pd.read_excel(path) 
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    insert_data(dim_users, conn)

if __name__=='__main__':
    main(path_to_drive + r'stage/dim_user.xlsx')