# Load libraries
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

os.chdir(path)
sys.path.insert(0, path)
from lib.general_module import get_conn, insert_batch

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")


##### Dependencies: E_ideas_form_field_answers.py > T_ideas.py > T_fact_sub_idea.py ###############



###############################ENVIRONMENT VARIABLES#####################################
load_dotenv()
aws_host = os.environ.get('aws_host')
aws_db = os.environ.get('aws_db')
aws_db_dw = os.environ.get('aws_db_dw')
aws_db = os.environ.get('aws_db')
aws_port = int(os.environ.get('aws_port'))
aws_user_db = os.environ.get('aws_user_db')
aws_pass_db = os.environ.get('aws_pass_db')
path_to_drive = os.environ.get('path_to_drive')

######################## Main FUNCTIONS #########################################


insert_msg = """INSERT INTO public.fact_submitted_idea (idea_id, company_id, user_id_1, \
    user_id_2, user_id_3, user_id_4, users, goal_id, submitted_at) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """


def main(path):
    
    fact_sub_idea = pd.read_parquet(path)
    print(fact_sub_idea.tail())
    conn = get_conn(aws_host, aws_db_dw, aws_port, aws_user_db, aws_pass_db)
    insert_batch(fact_sub_idea, conn, insert_msg, 100)
    return None

if __name__=='__main__':
    path = sys.argv[1]    
    main(path) 