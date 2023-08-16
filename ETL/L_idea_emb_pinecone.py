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
#Create dictionary from DIM_IDEAS table

conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
query = "Select id, idea_db_id from innk_dw_dev.public.dim_idea"
result = execute_sql(query, conn)
idea_dic = pd.DataFrame(result, columns=['id', 'idea_db_id']).set_index('idea_db_id')['id'].to_dict()

##########################AUXILIARY FUNCTIONS########################################

def average_valid_vectors(row:pd.Series, columns:list):
    """This function takes a row and a list of columns and returns the average of the non-null vectors in the row.

    Args:
        row (row): _description_
        columns (list): embedded columns

    Returns:
        np.vector: average of the non-null vectors in the row
    """
    valid_vectors = [vec for vec in [row[col] for col in columns] if not pd.isna(vec).all()]
    
    if not valid_vectors:  # Check if list is empty
        return np.nan
    else:
        return np.mean(valid_vectors, axis=0)



#pinecone.init(api_key=PINECONE_API_KEY, environment='us-west1-gcp')
#index = pinecone.Index(index_name=PINECONE_INDEX_IDEA)

df_dim = pd.read_parquet(path_to_drive + 'stage/dim_idea.parquet')
df_dim['idea_dw_id'] = df_dim['idea_db_id'].apply(lambda x: idea_dic[x])

df_stage = df_dim[['idea_dw_id', 'name_embedded','prob_1_embedded', 'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded',
                   'prob_3_embedded', 'sol_3_embedded','prob_4_embedded', 'sol_4_embedded', 'prob_5_embedded', 
                   'sol_5_embedded','prob_6_embedded', 'sol_6_embedded','idea_db_id']]

#make avarefe of vector in order to leave just one.

embedded_columns = ['name_embedded', 'prob_1_embedded', 'sol_1_embedded', 'prob_2_embedded', 'sol_2_embedded', 
                    'prob_3_embedded', 'sol_3_embedded', 'prob_4_embedded', 'sol_4_embedded', 'prob_5_embedded', 
                    'sol_5_embedded', 'prob_6_embedded', 'sol_6_embedded']

df_stage['combined_emb'] = df_stage.apply(lambda row: average_valid_vectors(row, embedded_columns), axis=1)


#barch insert
import random
import itertools

def chunks(iterable, batch_size=100):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))

vector_dim = 128
vector_count = 10000

# Example generator that generates many (id, vector) pairs
example_data_generator = map(lambda i: (f'id-{i}', [random.random() for _ in range(vector_dim)]), range(vector_count))

# Upsert data with 100 vectors per upsert request
for ids_vectors_chunk in chunks(example_data_generator, batch_size=100):
    index.upsert(vectors=ids_vectors_chunk)  # Assuming `index` defined elsewhere