import openai
from dotenv import load_dotenv
import os
import pandas as pd
import ast
import time
from openai.error import APIError
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
print(path)
from lib.general_module import get_conn, get_embeddings, ensure_columns, categorize, execute_sql
from lib.openai_module import classify_field

load_dotenv()
path_to_drive = os.environ.get('path_to_drive')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY_INNK') 
OPENAI_ORG_ID = os.environ.get('OPENAI_ORG_ID_INNK')
openai.api_key = OPENAI_API_KEY
openai.organization = OPENAI_ORG_ID



df =  pd.read_excel(path_to_drive + r'cluster_117.xlsx')#dataframe with idea name, idea_id, goal_id, name and embedded column with a mean of all the embedded columns of the idea
df.rename(columns={'solution_1':'description'}, inplace=True)


def classify_cluster(df:pd.DataFrame)->dict:
    """
    Classify a cluster of ideas with its descriptions and name the cluster, the ideas and give a short description of each idea.
    
    Args:
        df (pd.DataFrame): Filtered dataframe of a cluster of ideas with its descritions

    Returns:
        dict: chat gpt answer for naming the cluster, giving a name to the idea and a short description
    """
    
    base_message = f"I have the following cluster of ideas with it name and description. Need to resume each idea\
        in one word, then a small description of no more than 25 words, and finally a name for the whole cluster. \
        Give the answer in the same order of the list. Each idea is wrapped between [ ] This is the cluster: \n"
        
    cluster_text = ''
    
    end_message = "\n return only a dictionary ready to be parsed in python in spanish. The dictionary should have a key cluster_name \
    and a value with your answer, a key with cluster_description and a value your answer. Then a key with the number of the idea, and for value \
    a dictionary with key name and value with your answer for the idea name and another key description with you answer for the idea description."
    
    for i, row in df.iterrows():
        text = '[Idea ' + str(i) + ': Name: ' + str(df.loc[i,'name']) + '| Description: ' + str(df.loc[i,'description']) + "]\n"
        cluster_text += text
        
    message = base_message + cluster_text + end_message
    
    
    response = openai.ChatCompletion.create(
            model ="gpt-3.5-turbo",
            messages = [{"role": "system", 
                          "content": "You are a helpful assistant."}, 
                         {"role": "user", "content": message}],
            max_tokens=1000,
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
    df.loc[:,'cluster_name'] = answer['cluster_name']
    df.loc[:,'cluster_description'] = answer['cluster_description']
    for i, row in df.iterrows():
        df.loc[i,'idea_name'] = answer[str(i)]['name']
        df.loc[i,'idea_description'] = answer[str(i)]['description']
    return df


a = classify_cluster(df.loc[df.loc[:,'cluster']==0])
b = fill_cluster(df.loc[df.loc[:,'cluster']==0], a)
