#Modulo de funciones generales para ETL
import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
import redshift_connector

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_port = os.environ.get('aws_port')
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')

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

def connect_db(credenc:dict):
    """Crea conexión con base de datos. Datos sobre la db a la que se conecta (dev,
    dock o prod), viene dado por el diccionario que probablemente viene de funcion
    get_credentials a la que se le debe especificar el ambiente.
    Args:
        credenc (dict): diccionario con credenciales de acceso a db
    Returns:
       engine: engine for db connection
    """
    dbname = credenc['dbname']
    user = credenc['user']
    password = credenc['password']
    host = credenc['host']
    port = credenc['port']       
    conexion = f'postgresql://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(conexion)
    return engine