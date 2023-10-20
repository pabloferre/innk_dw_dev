# Load libraries
import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn



today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")
load_dotenv()

################################## ENVIRONMENT VARIABLES ################################
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
path_to_raw = os.environ.get('path_to_raw')

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
    print(len(df_final), len(df))
    return df_final

################################## MAIN FUNCTION #####################################

def main():
    
    conn = get_conn(aws_host, aws_db, aws_port, aws_user_db, aws_pass_db)
    conn2 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    df = get_companies(conn, conn2)
    path_to_folder = path_to_raw + str(today)
    os.makedirs(path_to_folder, exist_ok=True)
    final_path = path_to_folder + r'/companies.json'
    df.to_json(final_path, orient='records')
    
    return final_path

if __name__ == '__main__':
    main()