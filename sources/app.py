import streamlit as st
import pandas as pd
from gensim import corpora
from gensim.models import LdaModel
from gensim.parsing.preprocessing import STOPWORDS
from gensim.parsing.porter import PorterStemmer

# Cargar el DataFrame
df = pd.read_csv("..\datasets\csv\sentiment_analysis.csv")
df.drop('Unnamed: 0', axis=1, inplace=True)

# Preprocesamiento de texto
stopwords = set(STOPWORDS)
porter_stemmer = PorterStemmer()

def preprocess_text(text):
    # Agregar aquí cualquier otro preprocesamiento que desees aplicar
    processed_text = [porter_stemmer.stem(word) for word in text.lower().split() if word not in stopwords]
    return processed_text

# Aplicar preprocesamiento al texto
df['processed_text'] = df['text'].apply(preprocess_text)

# Crear un diccionario y corpus para el modelo LDA
dictionary = corpora.Dictionary(df['processed_text'])
corpus = [dictionary.doc2bow(text) for text in df['processed_text']]

# Entrenar el modelo LDA
lda_model = LdaModel(corpus, num_topics=5, id2word=dictionary)

# Crear filtros interactivos en Streamlit
st.sidebar.title("Filtros")
all_business_ids = df['business_id'].unique()
selected_business_ids = st.sidebar.multiselect("Seleccionar business_id:", all_business_ids)
all_stars = sorted(df['stars'].unique())
selected_stars = st.sidebar.selectbox("Seleccionar calificación:", ["Todas"] + all_stars)

# Filtrar el DataFrame según las selecciones del usuario
if not selected_business_ids and selected_stars == "Todas":
    filtered_df = df
else:
    if selected_business_ids:
        df = df[df['business_id'].isin(selected_business_ids)]
    if selected_stars != "Todas":
        df = df[df['stars'] == selected_stars]
    filtered_df = df

# Obtener las palabras clave de todos los tópicos en el conjunto de datos filtrados
all_topic_words = []
for topic in range(lda_model.num_topics):
    topic_words = lda_model.print_topic(topic, topn=10).split('+')
    topic_words = [word.split('*')[1].strip().strip('"') for word in topic_words]
    all_topic_words.append(topic_words)

# Mostrar los resultados en Streamlit
st.title("Palabras clave de Tópicos en el Conjunto de Datos Filtrados")
st.write(f"Mostrando palabras clave para business_id: {selected_business_ids} y calificación: {selected_stars} estrellas")
for topic, topic_words in enumerate(all_topic_words):
    st.write(f"Tópico {topic}: {topic_words}")



