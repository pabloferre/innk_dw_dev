# Load libraries
import os
import sys
import traceback
import time
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import pinecone
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.general_module import get_conn, categorize, execute_sql

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
PINECONE_API_KEY = os.environ.get('PINECONE_API_KEY')
PINECONE_INDEX_IDEA = os.environ.get('PINECONE_INDEX_IDEA')
path_to_drive = os.environ.get('path_to_drive')

######################## AUXILIARY DICTIONARIES #########################################
conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, idea_db_id from innk_dw_dev.public.dim_idea"
result = execute_sql(query, conn)
idea_dic = pd.DataFrame(result, columns=['id', 'idea_db_id']).set_index('idea_db_id')['id'].to_dict()

#########################################################################################

#Create dictionary from DIM_IDEAS table




#pinecone.init(api_key=PINECONE_API_KEY, environment='us-west1-gcp')
#index = pinecone.Index(index_name=PINECONE_INDEX_IDEA)

df_dim = pd.read_parquet(path_to_drive + 'stage/dim_idea.parquet')
df_dim['idea_dw_id'] = df_dim['idea_db_id'].apply(lambda x: idea_dic[x])

df_stage = df_dim[['idea_dw_id', 'name_embedded','prob_1_embedded', 'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded',
                   'prob_3_embedded', 'sol_3_embedded','prob_4_embedded', 'sol_4_embedded', 'prob_5_embedded', 
                   'sol_5_embedded','prob_6_embedded', 'sol_6_embedded','idea_db_id']]

#make avarefe of vector in order to leave just one.

df_stage = df_stage.loc[~df_stage.loc[:,'sol_1_embedded'].isnull()]
df2['combined_emb'] = df2.apply(lambda x: np.mean([x['name_embedded'], x['sol_1_embedded']], axis=0), axis=1)
combined_emb = np.array(df2['combined_emb'].to_list())