import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import streamlit as st

# Cargar datos desde el archivo CSV
file_path = 'topics.csv'
df = pd.read_csv(file_path)

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
