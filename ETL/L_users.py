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

crdenc = {'dbname': aws_db_dw, 'user': aws_user_db, 'password': aws_pass_db, 'host': aws_host, 'port': aws_port}



def insert_data(df, conn):
    """Insert data into ideas table in database"""

    insert = """INSERT INTO innk_dw_dev.public.dim_users (users, company_id, idea_db_id, user_db_id_1, user_name_1, user_last_name_1, \
        user_email_1, user_position_1, \
        user_contract_profile_1, user_area_1, user_sub_area_1, user_country_1, user_db_id_2, user_name_2, user_last_name_2, user_email_2, \
        user_position_2, user_contract_profile_2, user_area_2, user_sub_area_2, user_country_2, user_db_id_3, user_name_3, user_last_name_3, \
        user_email_3, user_position_3, user_contract_profile_3, user_area_3, user_sub_area_3, user_country_3, user_db_id_4, user_name_4, \
        user_last_name_4, user_email_4, user_position_4, user_contract_profile_4, user_area_4, user_sub_area_4, user_country_4,\
        valid_from, valid_to, is_current\
    ) \
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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


def main(path = r'H:\Mi unidad\Innk\dim_users.xlsx'): #CAMBIAR  EN ETL FINAL
    #engine = connect_db(crdenc)
    dim_users = pd.read_excel(path) 
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    insert_data(dim_users, conn)

    conn.close()

if __name__=='__main__':
    main()