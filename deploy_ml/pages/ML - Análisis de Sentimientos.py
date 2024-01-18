import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import streamlit as st
from google.cloud import bigquery

'''Conexión a BigQuery'''

# Configura tu proyecto y credenciales
project_id = 'final-project-data-insght-pro'
client = bigquery.Client(project=project_id)

# Especifica tu conjunto de datos y tabla para Yelp Reviews
dataset_id = 'ultabeautyreviews'
table_id = 'ulta_beauty_sentiment_analysis'


# Obtiene el esquema de la tabla de Yelp Reviews y Google Reviews
table = client.get_table(f'{project_id}.{dataset_id}.{table_id}')

# Construye y ejecuta la consulta para Yelp Reviews
queryY = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
query_jobY = client.query(queryY)
df=query_jobY.to_dataframe()


"""Generación WordCloud"""

# Filtrar datos para obtener solo sentimientos positivos y negativos
positive_reviews = df[df['sentiment_label'] == 'positive']['text'].astype(str)
negative_reviews = df[df['sentiment_label'] == 'negative']['text'].astype(str)

# Función para generar nube de palabras
def generate_wordcloud(data, title, background_color='white'):
    wordcloud = WordCloud(width=800, height=400, background_color=background_color).generate(' '.join(data))
    fig, ax = plt.subplots()
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.axis('off')
    ax.set_title(title)
    st.pyplot(fig)

# Crear nube de palabras para sentimientos positivos con fondo verde claro
st.header('Nube de Palabras')
generate_wordcloud(positive_reviews, 'Sentimientos Positivos', background_color='lightgreen')

# Crear nube de palabras para sentimientos negativos con fondo rosado claro
#st.header('Nube de Palabras para Sentimientos Negativos')
generate_wordcloud(negative_reviews, 'Sentimientos Negativos', background_color='lightcoral')
