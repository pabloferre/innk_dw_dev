# Load libraries
import os
import sys
import traceback
import time
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import openai
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.general_module import get_conn, get_embeddings, ensure_columns, categorize, execute_sql

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

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


######################## AUXILIARY DICTIONARIES #########################################
#Nested dictionary for company forms
conn1 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query1 = "Select company_id, name, category from innk_dw_dev.public.param_class_table"
result1 =  execute_sql(query1, conn1)

class_dict = {}

df_d = pd.DataFrame(result1, columns=['company_id', 'name', 'category'])   

for index, row in df_d.iterrows():
    company_id = row['company_id']
    name = row['name']
    category = row['category']

    if company_id not in class_dict:
        class_dict[company_id] = {}

    class_dict[company_id][name] = category

#Create dictionary from DIM_COMPANY table
conn2 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query2 = "Select id, company_db_id from innk_dw_dev.public.dim_company"
result2 = execute_sql(query2, conn2)

comp_dic = pd.DataFrame(result2, columns=['d', 'company_db_id']).set_index('d')['company_db_id'].to_dict()   

#Create dictionary from DIM_USERS table
conn3 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query3 = "Select id, user_db_id from innk_dw_dev.public.dim_users"
result3 = execute_sql(query3, conn3)
user_dic = pd.DataFrame(result3, columns=['id', 'user_db_id']).set_index('user_db_id')['id'].to_dict() 

#Create dictionary from DIM_IDEAS table

conn4 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query4 = "Select id, idea_db_id from innk_dw_dev.public.dim_idea"
result4 = execute_sql(query4, conn4)
idea_dic = pd.DataFrame(result4, columns=['id', 'idea_db_id']).set_index('idea_db_id')['id'].to_dict()

#################################AUXILIARY FUNCTIONS############################################

def process_data(df:pd.DataFrame, start_index=0)->pd.DataFrame:
    """Function that processes the data frame and returns a new data frame with the embeddings"""
    length = len(df)
    for i, row in df.iterrows():
        index = i + start_index
        print(index)
        try:
            embedding = get_embeddings(row['field_answer'])
            # Save the embedding in a new column
            df.at[index, 'ada_embedded'] = np.array(embedding).tolist()
            # Sleep to respect API rate limits
            time.sleep(1)  # Adjust based on your rate limit
            if index == length:
                return df
        except openai.error.APIConnectionError:
            print(index)
            return df
        except ValueError:
            #IF THE VALUE ERROR IS RAISED, CHECK IF THE FINAL INDEX IS CORRECT WITH THE NUMBER OF ROWS
            print('ValueError, check if the final index is correct. Final index: ', index)
            return df
        except Exception as e:
            print(traceback.format_exc())
            print('Error in index:', index, 'Resuming in 2 seconds...')
            time.sleep(2)
            process_data(df, index)
    return df



def fill_prev_columns(row:pd.Series)->pd.Series:
    """Function that fills the previous columns with the last non-nan value

    Args:
        row (_type_): row of a data frame

    Returns:
        row: modify row
    """
    
    problem_columns = [f"problem_{i+1}" for i in range(6)]
    solution_columns = [f"solution_{i+1}" for i in range(6)]
    prob_emb_columns = [f"prob_{i+1}_embedded" for i in range(6)]
    sol_emb_columns = [f"sol_{i+1}_embedded" for i in range(6)]

    for columns in [problem_columns, solution_columns, prob_emb_columns, sol_emb_columns]:
        non_empty_cols = [col for col in columns if not pd.isnull(row[col])]
        # Shift values to the left
        for i, col in enumerate(columns):
            row[col] = row[non_empty_cols[i]] if i < len(non_empty_cols) else np.nan

    return row




#################################MAIN FUNCTION############################################

#Classify form field answers from database
df_forms_ans = pd.read_excel(path_to_drive + r'raw/ideas_form_field_answers.xlsx')
df_forms_ans = df_forms_ans.loc[df_forms_ans['company_id'].isin(class_dict.keys())]
df_forms_ans['category'] = df_forms_ans.apply(lambda x: categorize(x['title'], class_dict[x['company_id']]), axis=1)

#Get embeddeds from database
df_emb = df_forms_ans.loc[~df_forms_ans['category'].isin(['Other','None']),].copy().reset_index(drop=True)
df_emb['ada_embedded'] = None
df_emb = process_data(df_emb, 13255)
df_emb.rename(columns={'field_answer':'description'}, inplace=True)

