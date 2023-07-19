# Load libraries
import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
from lib.general_module import get_conn

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")
path_to_file = r'H:\Mi unidad\Innk\T_classification.xlsx'  #BORRAR cuando se haga el ETL
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')




def insert_data_ctable(df:pd.DataFrame, conn:object):
    """Insert data into classification table in database

    Args:
        df (pd.DataFrame): data frame
        conn (object): connection to database
    """
    with conn.cursor() as cur:
        insert = 'INSERT INTO innk_dw_dev.public.param_class_table (company_id, name, description,\
            category) VALUES(%s, %s, %s, %s)'
        data = list(df.itertuples(index=False, name=None))
        print(data)
        # Execute the batch insert
        cur.executemany(insert, data)
        cur.close()
        # Commit the changes
        conn.commit()
        
    return None



def main(path_to_file:str):
    """Main function that prepares the classification table for the database"""

    class_table = pd.read_excel(path_to_file)
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    insert_data_ctable(class_table, conn)
    conn.close()
    return None


if __name__=='__main__':
    path_to_file = sys.argv[1]
    main(path_to_file)
