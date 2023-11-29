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
OPENAI_MODEL = os.environ.get('OPENAI_MODEL')

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

def execute_sql(query:str, conn:redshift_connector):
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
    conn.close()
    return result
    
def average_valid_vectors(row: pd.Series, columns: list):
    vectors = []
    expected_length = 1536
    for col in columns:
        if row[col] is not None and type(row[col]) != float:
            arr = np.array(row[col])
            if len(arr) != expected_length:
                continue
            vectors.append(arr)

    if not vectors:
        return np.nan

    stacked_vectors = np.vstack(vectors)
    mean_vector = np.nanmean(stacked_vectors, axis=0)

    return mean_vector

def serialize_vector(vector:np.array)->str:
    """Serialize a vector to a string

    Args:
        vector (np.array): embedded vector

    Returns:
        str: string representation of a embedded vector
    """
    if isinstance(vector, (float, int)) and pd.isna(vector):  # Check if scalar and NaN
        return None
    elif isinstance(vector, np.ndarray) and np.isnan(vector).all():  # Check if all values in the numpy array are NaN
        return None
    elif vector is None:
        return None
    return ','.join(map(str, vector))

def deserialize_vector(vector_str:str)->np.ndarray:
    """Deserialize a vector from a string   

    Args:
        vector_str (str): string representation of a embedded vector

    Returns:
        np.ndarray: embeded vector
    """
    if vector_str == None:
        return np.nan
    return np.fromstring(vector_str, sep=',')

def insert_batch(df:pd.DataFrame, conn:redshift_connector, insert:str, batch_size=100):
    """Insert a small batch of rows from the DataFrame into the ideas table"""
    # Take a subset of your DataFrame
    subset_df = df.head(batch_size)
    data = list(subset_df.itertuples(index=False, name=None))
    
    total_rows = len(df)
    num_batches = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
    MAX_RETRIES = 10
    with conn.cursor() as cur:
        for i in range(num_batches):
            retries = 0
            success = False
            start_idx = i * batch_size
            end_idx = start_idx + batch_size
            batch_data = df.iloc[start_idx:end_idx]
            data = list(batch_data.itertuples(index=False, name=None))
            while retries < MAX_RETRIES and not success:
                try:
                    cur.executemany(insert, data)
                    conn.commit()
                    print(f"Inserted batch {i + 1} of {num_batches}")
                    success = True
                except Exception as e:
                    print("Error on batch number", i+1)
                    print("Error:", e)
                    retries += 1
                    if retries == MAX_RETRIES:
                        raise e

    conn.close()
        
def check_existing_records_ideas(df, conn):
    """Check if records in the DataFrame already exist in the database"""
    existing_ids = set()

    # Fetch existing idea_db_ids from the database
    with conn.cursor() as cur:
        cur.execute("SELECT idea_db_id FROM public.dim_idea")
        existing_ids.update(row[0] for row in cur.fetchall())

    # Filter out existing records from the DataFrame
    new_records = df[~df['idea_db_id'].isin(existing_ids)]
    return new_records