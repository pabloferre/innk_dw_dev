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
os.chdir(path)
from lib.general_module import get_conn, get_embeddings, ensure_columns, categorize, execute_sql

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")
path_to_file = r'H:\Mi unidad\Innk\T_classification.xlsx'  #BORRAR cuando se haga el ETL
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')


#################################### AUXILIARY FUNCTIONS ##########################################
    

def process_data(df, start_index=0):
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
        except ValueError:
            #IF THE VALUE ERROR IS RAISED, CHECK IF THE FINAL INDEX IS CORRECT WITH THE NUMBER OF ROWS
            print('ValueError, check if the final index is correct. Final index: ', index)
            return df
        except Exception as e:
            print(traceback.format_exc())
            print(index)
            return df
    return df



def fill_prev_columns(row):
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


################################ AUXILIARY DICTIONARIES ##########################################

f_ans_dict = {'Idea name_embedded':'name_embedded','Solution 1':'solution_1', 'Solution 2':'solution_2', 
        'Solution 3':'solution_3', 'Solution 4':'solution_4', 'Solution 5':'solution_5', 'Solution 6':'solution_6', 
        'Problem 1':'problem_1', 'Problem 2':'problem_2', 'Problem 3':'problem_3', 'Problem 4':'problem_4', 
        'Problem 5':'problem_5', 'Problem 6':'problem_6', 'Solution 1_embedded':'sol_1_embedded',
        'Solution 2_embedded':'sol_2_embedded', 'Solution 3_embedded':'sol_3_embedded',
        'Solution 4_embedded':'sol_4_embedded', 'Solution 5_embedded':'sol_5_embedded', 
        'Solution 6_embedded':'sol_6_embedded', 'Problem 1_embedded':'prob_1_embedded',
        'Problem 2_embedded':'prob_2_embedded', 'Problem 3_embedded':'prob_3_embedded',
        'Problem 4_embedded':'prob_4_embedded', 'Problem 5_embedded':'prob_5_embedded',
        'Problem 6_embedded':'prob_6_embedded'}

#Create dictionary from classification table
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
result2 =  execute_sql(query2, conn2)

comp_dic = pd.DataFrame(result2, columns=['d', 'company_db_id']).set_index('d')['company_db_id'].to_dict()   

#Create dictionary from DIM_USERS table
conn3 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query3 = "Select id, idea_db_id from innk_dw_dev.public.dim_users"
result3 =  execute_sql(query3, conn3)
user_dic = pd.DataFrame(result3, columns=['id', 'idea_db_id']).set_index('idea_db_id')['id'].to_dict() 
     
###################################################################################################


#Classify form field answers from database

df_f = pd.read_json(r'H:\Mi unidad\Innk\form_with_answers_117.json') #Cambiar en ETL final
df_f['category'] = df_f.apply(lambda x: categorize(x['field_title'], class_dict[x['company_id']]), axis=1)

#Get embeddeds from database
df_1 = df_f.loc[df_f['category']!='Other',].copy().reset_index(drop=True)
df_1['ada_embedded'] = None
df_1 = process_data(df_1)
df_1.rename(columns={'field_answer':'description'}, inplace=True)

#Get main idea table and tag_table to merge it with the embeddeds

idea_df = pd.read_json(r'H:\Mi unidad\Innk\ideas_table.json')
idea_df.rename(columns={'title':'name'}, inplace=True)

ideas_tags_df = pd.read_json(r'H:\Mi unidad\Innk\ideas_tags_table.json')
ideas_tags_df.rename(columns={'name':'tag_name', 'description':'tag_description'}, inplace=True)

df_stg = pd.merge(idea_df, df_1, how='right', right_on='idea_id', left_on='id')
df_stg = pd.merge(df_stg, ideas_tags_df, how='left', on='idea_id')
df_stg.rename(columns={'id':'idea_db_id'}, inplace=True)
stg_columns = ['idea_db_id', 'company_id_x', 'user_id', 'tag_name', 'tag_description', 'tag_type', 'is_private', 
                  'stage', 'like_ideas_count', 'average_general', 'name', 'description_y', 
                 'category_y','created_at', 'updated_at', 'submited_at','ada_embedded']
df_stg = df_stg[stg_columns]



# First, make a copy of the original dataframe
df_copy = df_stg.copy().reset_index()

# Then, apply the function to create new columns
for category in df_copy['category_y'].unique():
    df_copy[category] = df_copy.loc[df_copy['category_y'] == category, 'description_y']
    df_copy[str(category)+'_embedded'] = df_copy.loc[df_copy['category_y'] == category, 'ada_embedded']

# Replace all the NaNs with a suitable value, in this case, you can use an empty string
#df_copy.fillna('', inplace=True)

final_columns =['idea_db_id', 'company_id', 'user_id', 'tag_name', 'tag_description', 'tag_type', 'is_private', 
                  'stage', 'like_ideas_count', 'average_general', 'name', 'description', 
                 'category','created_at', 'updated_at', 'submited_at','name_embedded', 'solution_1', 'solution_2',
                 'solution_3', 'solution_4', 'solution_5', 'solution_6', 'problem_1', 'problem_2', 'problem_3', 'problem_4',
                 'problem_5', 'problem_6', 'sol_1_embedded', 'sol_2_embedded', 'sol_3_embedded', 'sol_4_embedded',
                 'sol_5_embedded', 'sol_6_embedded', 'prob_1_embedded', 'prob_2_embedded', 'prob_3_embedded',
                 'prob_4_embedded', 'prob_5_embedded', 'prob_6_embedded']

df_grouped = df_copy.groupby('idea_db_id').agg(lambda x: np.nan if x.isna().all() else x.dropna().iloc[0])
df_grouped.rename(columns=f_ans_dict, inplace=True)
df_grouped = ensure_columns(df_grouped, list(f_ans_dict.values()))
df_grouped.replace([None, ''], np.nan, inplace=True)
#df_grouped = df_grouped.apply(fill_prev_columns, axis=1)
df_grouped.reset_index(inplace=True)
df_grouped.rename(columns={'description_y':'description', 'company_id_x':'company_id', 'category_y':'category'},
                  inplace=True)
df_final = df_grouped[final_columns].apply(fill_prev_columns, axis=1)
df_final['valid_from'] = today
df_final['valid_to'] = '9999-12-31'
df_final['is_current'] = True

#################### SACAR ESTO CUANDO SE DEJE DE TRABAJAR CON EXCEL ############################## Parece que no
df_final['valid_from'] = df_final['valid_from'].dt.tz_localize(None)
df_final['created_at'] = df_final['created_at'].dt.tz_localize(None)
df_final['updated_at'] = df_final['updated_at'].dt.tz_localize(None)
df_final['submited_at'] = df_final['submited_at'].dt.tz_localize(None)
###################################################################################################

df_final_dim_idea = df_final[['idea_db_id', 'tag_name','tag_description', 'tag_type',
                'is_private','category', 'stage', 'like_ideas_count', 
                'average_general', 'name', 'description', 'problem_1', 'solution_1', 'problem_2', 
                'solution_2', 'problem_3', 'solution_3', 'problem_4', 'solution_4', 'problem_5', 
                'solution_5', 'problem_6', 'solution_6', 'name_embedded','prob_1_embedded', 'sol_1_embedded',
                'prob_2_embedded', 'sol_2_embedded', 'prob_3_embedded', 'sol_3_embedded','prob_4_embedded',
                'sol_4_embedded', 'prob_5_embedded', 'sol_5_embedded','prob_6_embedded', 'sol_6_embedded', 
                 'created_at', 'updated_at', 'valid_from', 'valid_to', 'is_current']]

df_final_fact_sub_idea = df_final[['idea_db_id', 'company_id', 'user_id', 'submited_at']]
df_final_fact_sub_idea.replace({pd.NaT: None}, inplace=True)
df_final_fact_sub_idea.loc[:,'company_id'] = df_final_fact_sub_idea.loc[:,'company_id'].apply(lambda x: categorize(x, comp_dic))
df_final_fact_sub_idea.loc[:,'user_id'] = df_final_fact_sub_idea.loc[:,'idea_db_id'].apply(lambda x: categorize(x, user_dic))


