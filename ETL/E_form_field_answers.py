# Load libraries
import os
import sys
import traceback
import time
import pandas as pd
import numpy as np
import re
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
from lib.general_module import get_conn, get_embeddings, ensure_columns



today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")
path_to_file = r'H:\Mi unidad\Innk\T_classification.xlsx'  #BORRAR cuando se haga el ETL
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')



def remove_illegal_chars(val):
    if isinstance(val, str):
        # removing characters that are not in the printable set of ASCII
        val = re.sub('[^\x20-\x7e]', '', val)
    return val


conn = get_conn(aws_host, aws_db, aws_port, aws_user_db, aws_pass_db)
comp = pd.read_json(r'H:\Mi unidad\Innk\companies.json')
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

df_combined = df_combined.applymap(remove_illegal_chars)
