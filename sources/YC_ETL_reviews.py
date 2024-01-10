#Importar librerias

import json
import pandas as pd
import polars as pl
import re

#Leer el archivo 

dfreviewsYelp=pl.read_ndjson(r"D:\Datasets_proyecto\review.json").to_pandas()

#Filtramos los negocios que son de ultabeauty

business_ids = dfbusinessYelp['business_id'].unique()
dfreviewsYelp= dfreviewsYelp[dfreviewsYelp['business_id'].isin(business_ids)]

#Borramos las columnas irrelevantes

dfreviewsYelp.drop(columns=['review_id','useful','funny','cool'],inplace=True)

#Obtenemos el año , mes y hora de la columna date

dfreviewsYelp['date'] = pd.to_datetime(dfreviewsYelp['date'], format='%Y-%m-%d %H:%M:%S')
dfreviewsYelp['month'] = dfreviewsYelp['date'].dt.month
dfreviewsYelp['year'] = dfreviewsYelp['date'].dt.year
dfreviewsYelp['hour'] = dfreviewsYelp['date'].dt.time
dfreviewsYelp.drop(columns=['date'],inplace=True)

#Filtramos los años a partir del 2019

dfreviewsYelp = dfreviewsYelp[(dfreviewsYelp['year'] >= 2019) & (dfreviewsYelp['year'] <= 2021)]

# Convertimos la columna texto a minuscula y quitamos los caracteres especiales

def limpiar_texto(texto):
    # Convertir a minúsculas
    texto = texto.lower()
    # Eliminar caracteres especiales usando expresiones regulares
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    return texto

# Aplicar la función a la columna 'text'
dfreviewsYelp['text'] = dfreviewsYelp['text'].apply(limpiar_texto)

#Agregamos la columna source 
dfreviewsYelp['source']='Y'