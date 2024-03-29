from dotenv import load_dotenv
import openai
import os
import pandas as pd
import time
import boto3
import sys
from datetime import datetime
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
import ast
import json
import time
from lib.utils import get_conn, execute_sql

load_dotenv()
bucket_name = os.environ.get('bucket_name')
aws_access_id = os.environ.get('aws_access_id')
aws_access_key = os.environ.get('aws_access_key')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY_INNK') 
OPENAI_ORG_ID = os.environ.get('OPENAI_ORG_ID_INNK')
OPENAI_MODEL = os.environ.get('OPENAI_MODEL')
openai.api_key = OPENAI_API_KEY
openai.organization = OPENAI_ORG_ID


today = datetime.today().strftime("%Y-%m-%d")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

################################Dependecies get_clusters.py ################################################


########################### PROMPT TEMPLATES #################################################

system_template = "You are a proficient assistant, experienced in categorizing and summarizing diverse ideas. \
You are tasked to succinctly summarize and classify the provided set of ideas. Please present your answer in \
a well-structured string representation of a dictionary."

# Prompt for renaming ideas and providing a short summary
prompt = """For each idea in the following list, please provide an index that is given by the user, a new inventive name (limited to 8 words maximum), 
and a concise summary (ranging from 10 to 35 words). Also probide a collective cluster name (up to 8 words) and 
description (from 10 words and up to 30 words) that represents all ideas. Only one cluster name and description for all ideas.

List of Ideas:
{ideas}

Example of the desired output:

{{
  "ideas": [
    {{
      "index": given index by user,
      "idea_name": "Innovative Name for Idea 0", 
      "idea_description": "A brief and engaging summary of Idea 0, not exceeding 35 words."
    }},
    {{
      "index": given index by user,
      "idea_name": "Creative Title for Idea 1", 
      "idea_description": "A succinct and clear summary for Idea 1, within 10 to 35 words."
    }}
    // Continue for each idea in the list
  ]]
  "cluster_name": "New Cluster Name",
    "cluster_description": "A short and concise summary of the cluster, not exceeding 30 words."
}}

And this is a example of unwanted output that also used double quotes in the text of the answer without escaping them:
```json
{{
  "ideas": [
    {{
      "index": given index by user,
      "idea_name": "Innovative Name for Idea 0", 
      "idea_description": "A brief and engaging summary of Idea 0, not exceeding 35 words."
    }},
    {{
      "index": given index by user,
      "idea_name": "Creative Title for Idea 1", 
      "idea_description": "A succinct and clear summary for "Idea 1", within 10 to 35 words."
    }}
    // Continue for each idea in the list
  ]]
  "cluster_name": "New Cluster Name",
    "cluster_description": "A short and concise summary of the cluster, not exceeding 30 words."
}}
```

Please avoid anythig that is not a dictionary with the desired output. Symbols like ```json, ``` and ```//``` are not allowed. Also, avoid 
using double or simple quotes in the text of you answer. Use the quotes only in the keys and values of the dictionary.

Note: Please ensure all new idea names and summaries are provided in Spanish.  
The summary for each idea should not exceed 35 words. A unique response is required for each individual idea in the list. Respond 
 with only a dicitionary with the disired output. No extra text or symbols.
"""

#prompt that considers previus cluster name and description
prompt_cluster = """For each idea in the following list, please provide an index that is given by the user, a new inventive name (limited to 8 words maximum), 
and a concise summary (ranging from 10 to 35 words). Also probide a collective cluster name (up to 8 words) and 
description (from 10 words and up to 30 words) that represents all ideas. Only one cluster name and description for all ideas.

List of Ideas:
{ideas}

Example of the desired output:

{{
  "ideas": [
    {{
      "index": given index by user,
      "idea_name": "Innovative Name for Idea 0", 
      "idea_description": "A brief and engaging summary of Idea 0, not exceeding 35 words."
    }},
    {{
      "index": given index by user,
      "idea_name": "Creative Title for Idea 1", 
      "idea_description": "A succinct and clear summary for Idea 1, within 10 to 35 words."
    }}
    // Continue for each idea in the list
  ]]
  "cluster_name": "New Cluster Name",
    "cluster_description": "A short and concise summary of the cluster, not exceeding 30 words."
}}

And this is a example of unwanted output that also used double quotes in the text of the answer without escaping them:
```json
{{
  "ideas": [
    {{
      "index": given index by user,
      "idea_name": "Innovative Name for Idea 0", 
      "idea_description": "A brief and engaging summary of Idea 0, not exceeding 35 words."
    }},
    {{
      "index": given index by user,
      "idea_name": "Creative Title for Idea 1", 
      "idea_description": "A succinct and clear summary for "Idea 1", within 10 to 35 words."
    }}
    // Continue for each idea in the list
  ]]
  "cluster_name": "New Cluster Name",
    "cluster_description": "A short and concise summary of the cluster, not exceeding 30 words."
}}
```

Please avoid anythig that is not a dictionary with the desired output. Symbols like ```json, ``` and ```//``` are not allowed. Also, avoid 
using double or simple quotes in the text of you answer. Use the quotes only in the keys and values of the dictionary.


Take in consideration to combine the cluster name and description from the previous chunk of ideas. 
This where the previous cluster name and description from the previous chunk of ideas:

Cluster name: {cluster_name}
Cluster description: {cluster_description}

Note: Please ensure all new idea names and summaries are provided in Spanish.  
The summary for each idea should not exceed 35 words. A unique response is required for each individual idea in the list. Respond 
 with only a dicitionary with the disired output. No extra text or symbols.
"""


# Prompt for generating a cluster name and description
cluster_name_description_template = """Provide a collective cluster name (up to 8 words) and \
description (from 10 words and up to 30 words) that represents all ideas below.
\n
List of Ideas:
\n
{ideas}
\nNote: Ensure the new cluster names and summaries are delivered in Spanish. One response is required for each idea \
listed. Don't copy the given idea name and idea description in the response. Avoid using simple or double quotes in the response."""


#################################################AUXILIARY FUNCTIONS##############################################
def chunk_dataframe(df, max_rows=5): 
    """
    Yields chunks of the dataframe with up to max_rows rows.
    """
    df = df.reset_index()
    for i in range(0, len(df), max_rows):
        yield df.iloc[i:i + max_rows]
        
        
def final_text(text):
    # Remove extra lines and strip spaces from each line
    try:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
    except AttributeError:
        return 'No data'
    # Join the cleaned lines
    cleaned_text = ' '.join(lines)
    # Remove extra spaces
    cleaned_text = ' '.join(cleaned_text.split())
    cleaned_text = cleaned_text.replace('"', '').replace("'", '')
    # Remove text len greater than 500
    if len(cleaned_text) > 800:
        cleaned_text = cleaned_text[:800]
    return cleaned_text

def create_message_prompt(ideas: pd.Series, prompt: str, flag:dict) -> str:
    # This function creates a message prompt from a list of ideas
    ideas_list = []
    for i, idea in enumerate(ideas):
        ideas_list.append(f"{i}.'index': {idea['index']}, 'idea_name': {final_text(idea['name'])}, 'idea_description': {final_text(idea['description'])}")
    ideas_str = '\n'.join(ideas_list)
    if flag['flag']:
        return prompt_cluster.format(ideas=ideas_str, cluster_name=flag['cluster_name'], cluster_description=flag['cluster_description'])
    else:
        return prompt.format(ideas=ideas_str)


def clean_json(text: str) -> str:
    text_ =  str(text).replace('```json', '').replace('```', '')
    return text_
  
def fix_json(json_str):
  # This function fixes the json string returned by the API, it ensures that the json string 
  # is valid removing unwanted quotes
  fix_json = {'ideas':[]} 
  idea_names = json_str.split('"idea_name": ')
  indexes = json_str.split('"index": ')
  for i, idea in enumerate(idea_names):
    if i == 0:
      continue
    else:
      index = indexes[i].split(',')[0]
      idea_names[i] = idea_names[i].split(',')[0].replace('"', '').replace('\n', '').replace('}','').replace(']','').strip()
      idea_description = idea.split('"idea_description":')[1].split(',')[0].replace('"', '').replace('\n', '').replace('}','').replace(']','').strip()

    fix_json['ideas'].append({'index':index, 'idea_name':idea_names[i], 'idea_description':idea_description})
  
  fix_json['cluster_name'] = json_str.split('"cluster_name": ')[1].split(',')[0].replace('"', '').replace('\n', '').replace('}','').replace(']','').strip()
  fix_json['cluster_description'] = json_str.split('"cluster_description": ')[1].split(',')[0].replace('"', '').replace('\n', '').replace('}','').replace(']','').strip()
  
  return str(fix_json).replace("'", '"')


def get_param_clustered_ideas(conn) -> pd.DataFrame:
  """Connects to database and returns a dataframe with all ideas form the
  param cluster table.

  Args:
      conn (_type_): db connection


  Returns:
      pd.DataFrame: _description_
  """
  q = """SELECT idea_id, idea_db_id, company_id, goal_id FROM public.param_clustered_ideas"""
  result = execute_sql(q, conn)
  df = pd.DataFrame(result, columns=['idea_id', 'idea_db_id', 'company_id', 'goal_id'])
  
  return df
    



############################################# TASK FUNCTIONS #################################################


def rename_ideas(df_chunk: pd.DataFrame, flag:dict) -> pd.DataFrame:
    # This function renames ideas based on the ideas provided and the cluster name and description
    if flag['flag']:
        message = create_message_prompt(df_chunk.loc[:, ['index', 'name', 'description']].to_dict(orient='records'), prompt_cluster, flag)
    else:
        message = create_message_prompt(df_chunk.loc[:, ['index', 'name', 'description']].to_dict(orient='records'), prompt, flag)
    
    max_attempts = 0
    while max_attempts < 5:
        try:
            response = openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": system_template},
                    {"role": "user", "content": message}
                ],
                temperature=0,
            )
            max_attempts = 5
        except Exception as e:
            print('PRINT error: ', e)
            time.sleep(2)
            max_attempts += 1
    
    max_retries = 0
    while max_retries < 5:
        json_data = {'ideas': [{'index':'', 'idea_name':'', 'ideas_description':''}], 
                     'cluster_name': 'None', 'cluster_description': 'None'}
        try:
            json_data = ast.literal_eval(clean_json(f"""{response.choices[0].message['content']}"""))
            max_retries = 5
        except Exception as e:
            print('PRINT error ', e, response.choices[0].message['content'])
            try:
                json_data = json.loads(fix_json(response.choices[0].message['content']))
                max_retries = 5
            except:
                max_retries += 1

    df_ = pd.DataFrame(json_data['ideas'])
    df_['cluster_name'] = json_data['cluster_name']
    df_['cluster_description'] = json_data['cluster_description']
    
    renamed_ideas = df_chunk.merge(df_, on='index', how='left')
    return renamed_ideas

        


########################### PROCESSING FUNCTIONS #################################################

def process_cluster(cluster_df: pd.DataFrame) -> pd.DataFrame:
    # This function processes a single cluster
    # Rename ideas
    renamed_ideas = pd.DataFrame()
    flag = {'flag':False, 'cluster_name':'', 'cluster_description':''}
    for chunk in chunk_dataframe(cluster_df):
        chunk_result = rename_ideas(chunk, flag)
        flag['cluster_name'] = chunk_result.loc[0, 'cluster_name']
        flag['cluster_description'] = chunk_result.loc[0, 'cluster_description']
        flag['flag'] = True
        
        renamed_ideas = pd.concat([renamed_ideas, chunk_result], ignore_index=True)
    # Combine results into a DataFrame
    return renamed_ideas

def process_all_clusters(df: pd.DataFrame) -> pd.DataFrame:
    # This function processes all clusters in the DataFrame
    all_clusters_df = pd.DataFrame()
    
    for company_id in df['company_id'].unique():
        company_df = df[df['company_id'] == company_id]  # Filter the dataframe for the current company_id

        for cluster_num in company_df['cluster_number'].unique():
            # Filter the dataframe for the current cluster_number within the company_id
            cluster_df = company_df[company_df['cluster_number'] == cluster_num]
            
            # Process each cluster and combine results
            cluster_result_df = process_cluster(cluster_df)

            all_clusters_df = pd.concat([all_clusters_df, cluster_result_df], ignore_index=True)
            time.sleep(1)  # Sleep if necessary between processing clusters
    return all_clusters_df



############################################# MAIN FUNCTION #################################################

def main(url):

    s3_client = boto3.client('s3',
                    aws_access_key_id=aws_access_id,
                    aws_secret_access_key=aws_access_key)

    
    s3_file_name =  url.split('.com/')[-1]
    
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_json(response.get("Body"))

    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        raise Exception("Unsuccessful S3 get_object response")
    
    df.loc[~df.loc[:,'solution_1'].isnull(), 'description'] = df.loc[~df.loc[:,'solution_1'].isnull(), 'solution_1'] 
    
    df = df.reset_index()
    df_ = process_all_clusters(df)
    
    raw_file = df_.to_json(orient='records')
    
    s3_file_name_raw = 'raw/' + str(today) + '_param_clustered_ideas.json'
    
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name_raw, Body=raw_file)
    
    response = s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
    
    url = f'https://{bucket_name}.s3.amazonaws.com/{s3_file_name_raw}'
    
    print(url)
    
    sys.stdout.write(url)
    
    return url
    


if __name__ == '__main__':
    url = sys.argv[1]
    main(url)


