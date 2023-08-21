import openai
from dotenv import load_dotenv
import os
import pandas as pd
import ast
import time
from openai.error import APIError
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
from lib.general_module import get_conn, get_embeddings, ensure_columns, categorize, execute_sql
from lib.openai_module import classify_field

load_dotenv()
path_to_drive = os.environ.get('path_to_drive')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY_INNK') 
OPENAI_ORG_ID = os.environ.get('OPENAI_ORG_ID_INNK')
openai.api_key = OPENAI_API_KEY
openai.organization = OPENAI_ORG_ID



###########################AUXILIARY FUNCTIONS#################################################

def create_message(df:pd.DataFrame)->str:
    
    base_message = """I have the following cluster of ideas with it name and description. \
    return only a python dictionary. The dictionary should be as the \
    following json:  { 'cluster_name': 'you_asnwer for the cluster name in no more than 4 words', \
    'cluster_description': 'you_asnwer for the cluster name in no more than 4 words', '1': {'name': \
    'your answer for the idea number 1 name', 'description': 'you answer for idea number 1 description'} } \
    This is the cluster: \n"""
        
    cluster_text = ''
    
    end_message = "Here is an example: {'cluster_name': 'Oferta turística y comercial', 'cluster_description': \
    'Ideas para mejorar la oferta turística y comercial en las estaciones', '214': {'name': 'Módulos comerciales en plazoleta', \
    'description': 'Implementar módulos para guías turísticos en la plazoleta de la estación San Javier.'} } \
    This would be an error: {'cluster_name': 'Oferta turística', 'cluster_description': 'musica', 'ideas': {'214': 'name': 'Nombre', \
        'description': 'Descripcion'}. Rembember, ONLY return the dictionary"
    
    for i, row in df.iterrows():
        text = '[Idea number ' + str(i) + ': Name: ' + str(df.loc[i,'name']) + '| Description: ' + str(df.loc[i,'description']) + "]\n"
        if len(text) >1000:
            text = '[Idea number ' + str(i) + ': Name: ' + str(df.loc[i,'name']) + '| Description: ' + str(df.loc[i,'description'])[:500] + "]\n"
        cluster_text += text
    
    return clean_text(base_message + cluster_text + end_message)


def classify_cluster(df:pd.DataFrame)->dict:
    message = create_message(df)
    response = openai.ChatCompletion.create(
            model ="gpt-3.5-turbo",
            messages = [{"role": "system", 
                          "content": "You are a helpful assistant."}, 
                         {"role": "user", "content": message}],
            temperature=0,
    )
    return ast.literal_eval(response.choices[0].message['content'])



def fill_cluster(df:pd.DataFrame, answer:dict)->pd.DataFrame:
    """
    Fill the dataframe with the answers of the cluster classification

    Args:
        df (pd.DataFrame): Filtered dataframe of a cluster of ideas with its descritions
        answer (dict): dictionary with the answers of the cluster classification

    Returns:
        pd.DataFrame: dataframe with the answers of the cluster classification
    """
    df['cluster_name'] = answer['cluster_name']
    df['cluster_description'] = answer['cluster_description']
    for i, row in df.iterrows():
        df.at[i,'idea_name'] = answer[str(i)]['name']
        df.at[i,'idea_description'] = answer[str(i)]['description']
    return df


def chunk_dataframe(df, max_rows=5): 
    """
    Yields chunks of the dataframe with up to max_rows rows.
    """
    for i in range(0, len(df), max_rows):
        yield df.iloc[i:i + max_rows]

def classify_clusters_from_chunks(chunks):
    """
    Process dataframe chunks and classify them.
    """
    all_answers = {}
    
    for chunk in chunks:
        chunk =pd.DataFrame(chunk)
        answer = classify_cluster(chunk)
        all_answers.update(answer)
        
    return all_answers

def clean_text(text):
    # Remove extra lines and strip spaces from each line
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    # Join the cleaned lines
    cleaned_text = ' '.join(lines)
    # Remove extra spaces
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text


df =  pd.read_excel(path_to_drive + r'cluster_117.xlsx')#dataframe with idea name, idea_id, goal_id, name and embedded column with a mean of all the embedded columns of the idea
df.rename(columns={'solution_1':'description', 'combined_labels':'cluster'}, inplace=True)
df.sort_values('cluster', inplace=True)
df = df.reset_index(drop=True)


df_ = pd.DataFrame()
n = 0
parsed_clusters = [i for i in range(29)]
for i in df['cluster'].unique():
    if i in parsed_clusters:
        continue
    print(n, i)
    if n == 30:
        break
    
    chunks = list(chunk_dataframe(df.loc[df.loc[:,'cluster']==i]))

    # Classify each chunk

    answers = classify_clusters_from_chunks(chunks)


    # Fill dataframe with answers
    df_stg = fill_cluster(df.loc[df.loc[:,'cluster']==i], answers)
    

    df_ = pd.concat([df_,df_stg])
    n += 1
    time.sleep(1)
