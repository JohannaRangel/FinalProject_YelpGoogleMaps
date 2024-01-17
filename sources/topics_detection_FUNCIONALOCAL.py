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

def query_google_data():
    # Construir y ejecutar la consulta
    queryG = f"""
        SELECT rg.user_id, rg.business_id, rg.stars, rg.text, rg.month, rg.year, rg.source,
               bg.city, bg.state
        FROM
            `{project_id}.{dataset_id}.{table_idRG}` rg
        JOIN
            `{project_id}.{dataset_id}.{table_idBG}` bg
        ON
            rg.business_id = bg.business_id
        LIMIT
            {max_rows}
    """
    return client.query(queryG).to_dataframe()

def query_yelp_data():
    # Construir y ejecutar la consulta
    queryY = f"""
        SELECT rg.user_id, rg.business_id, rg.stars, rg.text, rg.month, rg.year, rg.source,
               bg.city, bg.state
        FROM
            `{project_id}.{dataset_id}.{table_idRY}` rg
        JOIN
            `{project_id}.{dataset_id}.{table_idBY}` bg
        ON
            rg.business_id = bg.business_id
        LIMIT
            {max_rows}
    """
    return client.query(queryY).to_dataframe()

# Interfaz de usuario con Streamlit
st.title("Consulta Interactiva de Datos")

# Declaración de variables dentro de la barra lateral
max_rows = st.sidebar.number_input("Número Máximo de Filas", min_value=1, value=10)
project_id = st.sidebar.text_input("ID del Proyecto", value=project_id)

if st.sidebar.button("Realizar Consultas"):
    try:
        # Realizar consultas
        google_reviews = query_google_data()
        yelp_reviews = query_yelp_data()

        # Concatenar ambos DataFrames
        df = pd.concat([google_reviews, yelp_reviews], ignore_index=True)

        # Mostrar el DataFrame resultante
        st.dataframe(df.head())
        st.success("Consultas exitosas.")

    except ValueError as ve:
        st.error(f"Error: {ve}")
    except Exception as e:
        st.error(f"Error inesperado: {e}")

