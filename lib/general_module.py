#Modulo de funciones generales para ETL
import os
import openai
import time
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
import redshift_connector


path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_port = os.environ.get('aws_port')
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY_INNK')
OPENAI_ORG_ID = os.environ.get('OPENAI_ORG_ID_INNK')

openai.api_key = OPENAI_API_KEY
openai.organization = OPENAI_ORG_ID
today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")

def get_conn(aws_host, aws_db, aws_port, aws_user_db, aws_pass_db):
    """Get connection to Redshift database

    Args:
        aws_host (_type_): host
        aws_db (_type_): database/dw
        aws_port (_type_): port
        aws_user_db (_type_): user for database
        aws_pass_db (_type_): password for user

    Returns:
        conn: database connection
    """
    conn = redshift_connector.connect(
        host=aws_host,
        database=aws_db,
        port=aws_port,
        user=aws_user_db,
        password=aws_pass_db
    )
    return conn


def connect_db(aws_host, aws_db, aws_port, aws_user_db, aws_pass_db):
    """Crea conexión con base de datos. Datos sobre la db a la que se conecta (dev,
    dock o prod), viene dado por el diccionario que probablemente viene de funcion
    get_credentials a la que se le debe especificar el ambiente.
    Args:
        credenc (dict): diccionario con credenciales de acceso a db
    Returns:
       engine: engine for db connection
    """   
    conexion = f'redshift+psycopg2://{aws_user_db}:{aws_pass_db}@{aws_host}:{aws_port}/{aws_db}'
    engine = create_engine(conexion)
    return engine

def get_embeddings(text:str)->list:
    """Get embeddings for text

    Args:
        text (str): _description_

    Raises:
        e: _description_

    Returns:
        list: _description_
    """
    try:
        # Call the OpenAI API
        text = text.replace("\n", " ")
        response = openai.Embedding.create(model="text-embedding-ada-002", input=[text])
        # Extract the embeddings
        embedding = response["data"][0]["embedding"]
    except Exception as e:
        print(f"Error occurred while getting embedding for text: {text}\n{str(e)}")
        raise e
    return embedding


def ensure_columns(df:pd.DataFrame, columns:list)->pd.DataFrame:
    """Ensure that dataframe has all columns in list"""
    
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan
    return df

def categorize(cell:str, class_dict:dict)->str:
    """Function that categorizes a cell, if category is not found, then assigns 'None'
    this function is for catching the errors when the cell is not found in the dictionary while
    using the apply method.

    Args:
        cell (str): value from a cell in a data frame
        class_dict (dict): classification dictionary

    Returns:
        str: categorization of the cell
    """
    try:
        category = class_dict[cell]
    except KeyError:
        category = 'None'
    
    return category

def execute_sql(query:str, conn):
    """Execute sql query in database

    Args:
        query (str): sql query
        conn (object): connection to database

    Returns:
        result: result of query
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()
    return result