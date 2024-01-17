# -*- coding: utf-8 -*-

"""Librerías"""
import streamlit as st
import pandas as pd

import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Ruta al archivo JSON de credenciales de Google Cloud
credentials_path = "C:/Users/johan/Bootcamp_SoyHenry/PI_Final/service_account.json"  # Cambia esto con la ruta correcta

# Configurar el cliente de BigQuery con las credenciales desde el archivo JSON
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
client = bigquery.Client()

# Declaración de variables
max_rows = 10
project_id = 'windy-tiger-410421'

# Definir las tablas
dataset_id = 'UltaBeautyReviews'
table_idBG = 'google_business_data'
table_idRG = 'google_reviews_ulta_beauty'
table_idBY = 'yelp_business_data'
table_idRY = 'yelp_reviews_ulta_beauty'
'''
def query_google_data(filters):
    # Construir y ejecutar la consulta
    where_clause = generate_where_clause(filters, 'g_rg')
    queryG = f"""
        SELECT g_rg.user_id, g_rg.business_id AS google_business_id, g_rg.stars, g_rg.text, g_rg.month, g_rg.year, g_rg.source,
               g_bg.city, g_bg.state
        FROM
            `{project_id}.{dataset_id}.{table_idRG}` g_rg
        JOIN
            `{project_id}.{dataset_id}.{table_idBG}` g_bg
        ON
            g_rg.business_id = g_bg.business_id
        {where_clause}
        LIMIT
            {max_rows}
    """
    return client.query(queryG).to_dataframe()
'''
def query_google_data(filters):
    # Construir y ejecutar la consulta
    where_clause = generate_where_clause(filters)
    queryG = f"""
        SELECT business_id, stars, text, month, year, source
        FROM
            `{project_id}.{dataset_id}.{table_idRG}` 
        {where_clause}
        LIMIT
            {max_rows}
    """
    return client.query(queryG).to_dataframe()

'''
def query_yelp_data(filters):
    # Construir y ejecutar la consulta
    where_clause = generate_where_clause(filters, 'y_rg')
    queryY = f"""
        SELECT y_rg.user_id, y_rg.business_id AS yelp_business_id, y_rg.stars, y_rg.text, y_rg.month, y_rg.year, y_rg.source,
               y_bg.city, y_bg.state
        FROM
            `{project_id}.{dataset_id}.{table_idRY}` y_rg
        JOIN
            `{project_id}.{dataset_id}.{table_idBY}` y_bg
        ON
            y_rg.business_id = y_bg.business_id
        {where_clause}
        LIMIT
            {max_rows}
    """
    return client.query(queryY).to_dataframe()
'''

'''
def generate_where_clause(filters, alias=None):
    clauses = []
    for key, value in filters.items():
        if value is not None:
            column_name = f"{alias}.{key}" if alias else key
            # Asegurarse de que el valor de 'stars' se trate como un entero en la condición
            value = int(value) if key == "stars" else value
            clauses.append(f"{column_name} = '{value}'")
    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)
'''
def generate_where_clause(filters, alias=None):
    clauses = []
    for key, value in filters.items():
        if value is not None:
            column_name = f"{alias}.{key}" if alias else key
            # Asegurarse de que el valor de 'stars' se trate como un entero en la condición
            value = int(value) if key == "stars" else value
            clauses.append(f"{column_name} = {value}")
    if not clauses:
        return ""
    return "WHERE " + " AND ".join(clauses)

# Interfaz de usuario con Streamlit
st.title("Consulta Interactiva de Datos")

#Filtros Desplegable
#with st.sidebar.expander("Detección de Tópicos"):
filters = {
        # "google_business_id": st.text_input("Google Business ID"),  # Comentado temporalmente
        # "yelp_business_id": st.text_input("Yelp Business ID"),
        "stars": st.selectbox("Stars", [1, 2, 3, 4, 5]),
        "month": st.selectbox("Month", range(1, 13)),
        "year": st.text_input("Year"),
        "source": st.text_input("Source")
        #"city": st.text_input("City"),
        #"state": st.text_input("State"),
 }

#with st.sidebar.expander("Análisis de Sentimiento"):
#    st.header("Filtros para Análisis de Sentimiento")
    
#with st.sidebar.expander("KPI's"):
#    st.header("Filtros KPI's")
    
if st.sidebar.button("Realizar Consultas"):
    try:
        # Realizar consultas con filtros
        google_reviews = query_google_data(filters)
        #yelp_reviews = query_yelp_data(filters)

        # Concatenar ambos DataFrames
        #df = pd.concat([google_reviews, yelp_reviews], ignore_index=True)
        df = google_reviews

        # Aplicar modelo LDA al conjunto de datos resultante
        # (agrega tu código de modelo LDA aquí)

        # Mostrar el DataFrame resultante
        st.dataframe(df.head())
        st.success("Consultas exitosas.")

    except ValueError as ve:
        st.error(f"Error: {ve}")
    except Exception as e:
        st.error(f"Error inesperado: {e}")
        print(e)
