# Load libraries
import os
import sys
import pandas as pd
import re
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.general_module import get_conn



today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")
load_dotenv()

##################################ENVIRONMENT VARIABLES###############################
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
path_to_drive = os.environ.get('path_to_drive')

##################################AUXILIARY FUNCTIONS#################################

def remove_illegal_chars(val):
    if isinstance(val, str):
        # XML 1.0 legal characters: #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | 
        # [#x10000-#x10FFFF]
        # To simplify, we will keep: ( #x9 | #xA | #xD | [#x20-#x7E] ),
        # accented characters and usual printable characters
        val = re.sub(r'[^\x09\x0A\x0D\x20-\x7EÁÉÍÓÚáéíóú]', '', val)
    return val

def get_forms_with_answers(conn, comp: pd.DataFrame): #Mejorar la funcion sacando el loop con otra query
    
    '''This function returns a dataframe with the answers to the forms of the companies in the comp dataframe'''
    
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
    return df_combined

##################################AUXILARY DICIONARIES##################################

def main():
    conn = get_conn(aws_host, aws_db, aws_port, aws_user_db, aws_pass_db)
    comp = pd.read_json(path_to_drive + r'companies.json')
    df_combined = get_forms_with_answers(conn, comp)
    df_combined = df_combined.applymap(remove_illegal_chars)
    df_combined.to_excel(path_to_drive + r'raw/ideas_form_field_answers.xlsx', index=False)
    
    return None

if __name__ == '__main__':
    main()
