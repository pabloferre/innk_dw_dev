import requests
import streamlit as st
import pandas as pd
import duckdb
#from fastapi.testclient import TestClient
from st_aggrid import GridOptionsBuilder
from st_aggrid import AgGrid

API_URL = "http://127.0.0.1:3000"

# Config

def agrid_options(dataframe, page_size):
    grid_options_builder = GridOptionsBuilder.from_dataframe(dataframe)
    grid_options_builder.configure_pagination(enabled=True, paginationPageSize=page_size, paginationAutoPageSize=False)
    grid_options_builder.configure_default_column(floatingFilter=True, selectable=False)
    grid_options_builder.configure_grid_options(domLayout='normal')
    grid_options_builder.configure_selection("single")
    return grid_options_builder.build()

def load_excel_to_db(db):
    # Load excel file to database
    sheet = "data/DWH  campos de formularios (datos de prueba).xlsx"
    df_fields = pd.read_excel(sheet, sheet_name="campos IdeCo")
    df_answers = pd.read_excel(sheet, sheet_name="respuestas IdeCo")
    db.sql("DROP TABLE IF EXISTS campos")
    db.sql("DROP TABLE IF EXISTS answers")
    db.sql("CREATE TABLE campos AS SELECT * FROM df_fields")
    db.sql("CREATE TABLE answers AS SELECT * FROM df_answers")

def get_ideas():
    return db.sql("""
    SELECT
        *
    FROM
        campos
    LEFT JOIN
        answers
    ON
        campos.field_id = answers.field_id
    WHERE type = 'Solución'
    """).to_df()

def get_similar_ideas(company_id:str, description:str) -> list:
    """Get similar ideas from server, given a company_id and a description
    TODO: This should be replaced by a real call to a server
    """
    response = requests.get(f"{API_URL}/ideas/{company_id}/similar/description/?description={description}")
    return response.json()["matches"]

st.set_page_config(layout="wide", page_title = "Calidad de datos")

db = duckdb.connect('data/warehouse.db')
#load_excel_to_db(db)
df_ideas = get_ideas()

st.write("# Ideas")

AgGrid(df_ideas, agrid_options(df_ideas, 10))

st.write("# Buscar ideas similares")

col1, col2 = st.columns([1,2])

# Select company_id
with col1:
    company_id = st.selectbox("Compañía", df_ideas["company_id"].unique())

# Query server for similar ideas
with col2:
    description = st.text_area("Descripción de la idea", key="description", help="Este texto se compara con los campo tipo 'Solución' de las ideas")

if st.button("Buscar"):

    if description == "":
        # Form validation
        st.warning("No se ha ingresado una descripción")
        st.stop()
    else:
        df_similar_ideas = pd.DataFrame(get_similar_ideas(company_id, description))
        print(df_similar_ideas)
        
        # Join with ideas to get description and others.
        similar_ideas = duckdb.sql("""
        SELECT
            *
        FROM
            df_similar_ideas
        LEFT JOIN
            df_ideas
        ON
            df_similar_ideas.id = df_ideas.idea_id
        ORDER BY
            score DESC
        """).to_df()
        
        st.dataframe(similar_ideas)