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

load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')


######################## AUXILIARY FUNCTIONS #########################################

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


import pandas as pd
import psycopg2

# Connect to the Redshift database
conn = psycopg2.connect(database="your_database", user="your_username",
                        password="your_password", host="your_redshift_host", port="your_port")

# Read the data you wish to upload into a DataFrame
df_to_upload = pd.read_csv("your_data.csv")

# Use the COPY command to upload the data to a temporary staging table
# Make sure to create the temporary staging table in Redshift first
with conn.cursor() as cursor:
    copy_query = f"COPY your_staging_table FROM 's3://your_s3_bucket/your_data.csv' CREDENTIALS 'aws_access_key_id=YOUR_ACCESS_KEY;aws_secret_access_key=YOUR_SECRET_KEY' CSV;"
    cursor.execute(copy_query)

# Write the INSERT query with ON CONFLICT clause to handle updates and inserts
insert_query = """
    INSERT INTO your_main_table (idea_db_id, company_id, column1, column2, ...)
    SELECT idea_db_id, company_id, column1, column2, ...
    FROM your_staging_table
    ON CONFLICT (idea_db_id, company_id) DO NOTHING;
"""

# Execute the INSERT query to update/insert data into the main table
with conn.cursor() as cursor:
    cursor.execute(insert_query)

# Commit the changes and close the connection
conn.commit()
conn.close()





def main():
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    dim_idea = pd.read_excel(r'H:\Mi unidad\Innk\dim_idea.xlsx')
    dim_idea.drop(columns=['company_id', 'user_id'], inplace=True)
    insert_data(dim_idea, conn)
    
if __name__=='__main__':
    main() 