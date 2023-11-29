# Load libraries
import os
import sys
import traceback
import time
import itertools
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import pinecone
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.insert(0, path)
from lib.utils import get_conn, categorize, execute_sql, average_valid_vectors, deserialize_vector

today = datetime.today().strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

########################## Dependencies: T_goal.py | L_fact_sub_ideas.py  ###############


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
PINECONE_INDEX_NAME = os.environ.get('PINECONE_INDEX_NAME')
bucket_name = os.environ.get('bucket_name')



########################### Hardcoded variables #########################################
EMBEDDING_VECTOR_SIZE = 1536
namespace = 'ideas_embedded'

######################## AUXILIARY DICTIONARIES #########################################
#Create dictionary from DIM_goal table to get the db id from the dw id

conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, goal_db_id from innk_dw_dev.public.dim_goal"
result = execute_sql(query, conn)
goal_dic = pd.DataFrame(result, columns=['id', 'goal_db_id']).set_index('id')['goal_db_id'].to_dict()

#create a dictionaty from dim_company to get the db id from the dw id
conn2 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, company_db_id from innk_dw_dev.public.dim_company"
result = execute_sql(query, conn2)
comp_dic = pd.DataFrame(result, columns=['id', 'company_db_id']).set_index('id')['company_db_id'].to_dict()


##################################AUXILIARY FUNCTIONS#####################################

def get_pinecone_index():
    # initialize connection to pinecone (get API key at app.pinecone.io)
    pinecone.init(
        api_key=PINECONE_API_KEY,
        environment="us-west4-gcp"
    )

    # check if 'innk-dev' index already exists (only create index if not)
    if PINECONE_INDEX_NAME not in pinecone.list_indexes():
        pinecone.create_index(PINECONE_INDEX_NAME, dimension=EMBEDDING_VECTOR_SIZE)
    
    # connect to index
    return pinecone.Index(PINECONE_INDEX_NAME)



def chunks(iterable, batch_size=100):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


##########################DataFrame########################################

conn3 = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = """Select dim_idea.id, idea_db_id, name_embedded, prob_1_embedded, sol_1_embedded, prob_2_embedded, sol_2_embedded,
    prob_3_embedded, sol_3_embedded, prob_4_embedded, sol_4_embedded, prob_5_embedded, sol_5_embedded,
        prob_6_embedded, sol_6_embedded, f.goal_id, f.company_id 
    from innk_dw_dev.public.dim_idea
    left join innk_dw_dev.public.fact_submitted_idea as f on f.idea_id = dim_idea.id"""

embedded_columns = ['name_embedded', 'prob_1_embedded', 'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded', 
                'prob_3_embedded', 'sol_3_embedded', 'prob_4_embedded', 'sol_4_embedded', 'prob_5_embedded', 
                'sol_5_embedded', 'prob_6_embedded', 'sol_6_embedded']

df = pd.DataFrame(execute_sql(query, conn), columns=['id', 'idea_db_id', 'name_embedded', 'prob_1_embedded',
    'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded', 'prob_3_embedded', 'sol_3_embedded', 'prob_4_embedded',
    'sol_4_embedded', 'prob_5_embedded', 'sol_5_embedded', 'prob_6_embedded', 'sol_6_embedded', 'f.goal_id', 'f.company_id'])
df.rename(columns={'f.goal_id':'goal_id', 'f.company_id':'company_id'}, inplace=True)

df['company_db_id'] = df['company_id'].apply(lambda x: categorize(x, comp_dic))
df['goal_db_id'] = df['goal_id'].apply(lambda x: categorize(x, goal_dic))

for col in embedded_columns:
    df[col] = df[col].apply(lambda x: deserialize_vector(x))

df['combined_emb'] = df.apply(lambda row: average_valid_vectors(row, embedded_columns), axis=1)

df = df.loc[df.loc[:,'combined_emb'].notnull()].reset_index(drop=True)
df_stage = df[['idea_db_id', 'company_db_id', 'goal_db_id', 'combined_emb']].copy()

vectors = [{"id":str(row['idea_db_id']),"values":row['combined_emb'],"metadata":{"company_db_id": row['company_db_id'],
                                                'goal_db_id':row['goal_db_id']}} for i, row in df_stage.iterrows()]

# get index
index = get_pinecone_index()

# Upsert data with 100 vectors per upsert request
for ids_vectors_chunk in chunks(vectors, batch_size=100):
    index.upsert(vectors=ids_vectors_chunk, namespace=namespace) 