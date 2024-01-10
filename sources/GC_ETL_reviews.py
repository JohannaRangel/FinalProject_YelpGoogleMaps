#Importar librerias
import pandas as pd
import os
from datetime import datetime
import re

#Leer el archivo

directorio = "D:/Datasets_proyecto/reviews-estados"

dfs = []
for filename in os.listdir(directorio):
    if filename.endswith(".json"):
        filepath = os.path.join(directorio, filename)
        
        
        df = pd.read_json(filepath, lines=True)
        
        
        dfs.append(df)
dfreviewsGoogle = pd.concat(dfs, ignore_index=True)

#Filtrar solo los que sean de ultabeauty

business_ids_to_keep = dfbusinessGoogle['business_id'].unique()
dfreviewsGoogle = dfreviewsGoogle[dfreviewsGoogle['gmap_id'].isin(business_ids_to_keep)]

#Borrar columnas inecesarias

dfreviewsGoogle.drop(columns=['name','pics','resp'],inplace=True)

#Extraer el mes , año , hora

dfreviewsGoogle['time'] = pd.to_datetime(dfreviewsGoogle['time'], unit='ms')
dfreviewsGoogle['month'] = dfreviewsGoogle['time'].dt.month
dfreviewsGoogle['year'] = dfreviewsGoogle['time'].dt.year
dfreviewsGoogle['hour'] = dfreviewsGoogle['time'].dt.hour
dfreviewsGoogle.drop(columns=['time'],inplace=True)

# Filtrar las filas para los años 2019, 2020 y 2021
dfreviewsGoogle = dfreviewsGoogle[(dfreviewsGoogle['year'] == 2019) | (dfreviewsGoogle['year'] == 2020) | (dfreviewsGoogle['year'] == 2021)]

#Renombrar las columnas
dfreviewsGoogle = dfreviewsGoogle.rename(columns={'gmap_id': 'business_id', 'rating': 'stars'})

# Reordenar las columnas
column_order = ['user_id', 'business_id', 'stars', 'text', 'month', 'year', 'hour']
dfreviewsGoogle = dfreviewsGoogle[column_order]

# Eliminar filas donde la columna 'text' es nula
dfreviewsGoogle = dfreviewsGoogle.dropna(subset=['text'])

# Eliminar filas duplicadas en función de todas las columnas
dfreviewsGoogle = dfreviewsGoogle.drop_duplicates()

#La columna texto la convertimos a toda minuscula y quitamos caracteres especiales
def limpiar_texto(texto):
    if isinstance(texto, str):
        texto = texto.lower()
        texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return texto

dfreviewsGoogle['text'] = dfreviewsGoogle['text'].apply(limpiar_texto)


#Agregamos la columna source 
dfreviewsGoogle['source']='G'



