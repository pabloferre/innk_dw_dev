import pandas as pd
import redshift_connector
import os
import sys
import json
from datetime import datetime
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.append(path)

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")
path_to_cred = path + r"\documentation\credentials.json"

with open(path_to_cred) as f:
    cred = json.load(f)

def get_conn():
    conn = redshift_connector.connect(
        host=cred[0]['AWS']['host'],
        database=cred[0]['AWS']['db'],
        port=5439,
        user=cred[0]['AWS']['user_db'],
        password=cred[0]['AWS']['pass_db']
    )
    return conn



#Compañias
companies = pd.read_json(r'H:\Mi unidad\Innk\companies.json')
companies.rename({'id':'company_db_id', 'name':'company_name',\
    'status':'status_active'}, axis=1, inplace=True)
companies = companies[['company_db_id', 'company_name', 'status_active', 'created_at', 'updated_at']]

insert_query = """insert into innk_dw_dev.public.dim_company 
        (company_db_id, company_name, status_active, created_at, updated_at, valid_from, valid_to, is_current)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

try:
    conn = get_conn() 
    for i, row in companies.iterrows():
        value_tuple = (row['id_company_db'], row['company_name'], row['status_active'], row['created_at'], \
            row['updated_at'], today, '9999-12-31', True)
        cur = conn.cursor()
        cur.execute(insert_query, value_tuple)
        cur.close()
    conn.commit()
    conn.close()
except Exception as e:
    conn.close()
    raise e

#DIM_IDEAS
ideas = pd.read_json(r'H:\Mi unidad\Innk\forms_complete.json')
ideas = ideas.loc[ideas.loc[:,'company_id'].isin(companies['company_db_id'])]

fields_by_company = ideas[['company_id', 'field_title', 'field_description']].drop_duplicates(ignore_index=True)

unique_fields = ideas['field_title'].unique()


#Company id dic
comp = pd.read_sql_query("""select id, id_company_db from innk_dw_dev.public.dim_companies""", conn)
comp_dic = dict(zip(comp['id_company_db'], comp['id']))
cat = pd.read_sql_query("""select id, id_category_db from innk_dw_dev.public.dim_categories""", conn)
cat_dic = dict(zip(cat['id_category_db'], cat['id']))


#Categories
categories = pd.read_json(r'H:\Mi unidad\Innk\categories.json')

categories['company_id'] = categories['company_id'].map(comp_dic)
categories.rename({'id':'id_category_db', 'name':'category_name','description':'category_description',\
    'id_company':'company_id', 'status':'status_active'}, axis=1, inplace=True)
categories.fillna('NULL', inplace=True)
categories = categories.loc[categories['company_id'] != 'NULL']
insert_query = """insert into innk_dw_dev.public.dim_categories 
        (id_category_db, category_name, category_description, company_id, status_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        
for i, row in categories.iterrows():
    value_tuple = (row['id_category_db'], row['category_name'], row['category_description'], \
        row['company_id'], row['status_active'], row['created_at'],  row['updated_at'])
    cur = conn.cursor()
    cur.execute(insert_query, value_tuple)
    cur.close()
conn.commit()
conn.close()

#goals
goals = pd.read_json(r'H:\Mi unidad\Innk\goals.json')
goals['company_id'] = goals['company_id'].map(comp_dic)
goals['category_id'] = goals['category_id'].map(cat_dic)
goals.rename({'id':'id_goal_db', 'name':'goal_name', 'description':'goal_description'}, axis=1, inplace=True)
#goals.fillna(None, inplace=True)
goals = goals.loc[goals['company_id'] != 'NULL']

insert_query = """insert into innk_dw_dev.public.fact_goals \
        (id_goal_db, goal_name, goal_description, company_id, category_id, created_at, updated_at) \
        VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        
for i, row in goals.iterrows():
    value_tuple = (row['id_goal_db'], row['goal_name'], row['goal_description'], \
        row['company_id'], row['category_id'], row['created_at'],  row['updated_at'])
    cur = conn.cursor()
    cur.execute(insert_query, value_tuple)
    cur.close()
conn.commit()
conn.close()


#ideas
gl = pd.read_sql_query("""select id, id_goal_db from innk_dw_dev.public.fact_goals""", conn)
gl_dic = dict(zip(gl['id_goal_db'], gl['id']))

ideas = pd.read_json(r'H:\Mi unidad\Innk\ideas.json')
ideas['company_id'] = ideas['company_id'].map(comp_dic)
ideas['goal_id'] = ideas['goal_id'].map(gl_dic)
ideas.rename({'id':'id_idea_db', 'title':'idea_name', 'description':'idea_description'}, axis=1, inplace=True)
#ideas.fillna('NULL', inplace=True)
ideas = ideas.loc[ideas['company_id'] != 'NULL']

insert_query = """insert into innk_dw_dev.public.fact_ideas \
        (id_idea_db, company_id, goal_id, idea_name, idea_description,   created_at, updated_at) \
        VALUES (%s, %s, %s, %s, %s, %s, %s);"""
n = 0
for i, row in ideas.iterrows():
    value_tuple = (row['id_idea_db'], row['company_id'], row['goal_id'], row['idea_name'], \
        row['idea_description'], today,  today)
    cur = conn.cursor()
    cur.execute(insert_query, value_tuple)
    cur.close()
    n += 1
    print(i)
    if n == 100:
        n = 0
        conn.commit()
conn.close()
              



#Ideas tags
ide_ = pd.read_sql_query("""select id, id_idea_db from innk_dw_dev.public.fact_ideas""", conn) 
ide_dic = dict(zip(ide_['id_idea_db'], ide_['id']))
ideas_tags = pd.read_json(r'H:\Mi unidad\Innk\ideas_tags.json')
ideas_tags['idea_id'] = ideas_tags['idea_id'].map(ide_dic)
ideas_tags['company_id'] = ideas_tags['company_id'].map(comp_dic)
ideas_tags.rename({'id':'id_idea_tag_db', 'name':'tag_name', 'description':'tag_description'}, axis=1, inplace=True)

ideas_tags.to_sql('dim_ideas_tags', con=conn, index=False, if_exists='append', method='multi', chunksize=1000)
