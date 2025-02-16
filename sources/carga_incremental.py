import pandas as pd

import requests
from datetime import datetime
import re
from io import StringIO
import datetime
from google.cloud import bigquery
from google.cloud import storage
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
import numpy as np
def datos_nuevos_Google():
    # URL de la API con el token
    api_url = "https://api.apify.com/v2/actor-tasks/elgabb01~google-maps-scraper-task/runs/last/dataset/items?token=apify_api_RBIv1NkT8xL34eWjtg8UaSZsaWj6D02uuC8u&unwind=reviews&fields=placeId,reviews&omit=textTranslated,publishAt,likesCount,reviewId,reviewUrl,reviewerUrl,reviewerPhotoUrl,reviewerNumberOfReviews,isLocalGuide,rating,reviewImageUrls,reviewContext,reviewDetailedRating,responseFromOwnerDate,name,responseFromOwnerText"

    # Realiza la solicitud GET
    response = requests.get(api_url)

    # Verifica el estado de la respuesta
    if response.status_code == 200:
        # Parsea la respuesta JSON
        data = response.json()

        # Imprime los datos (o haz lo que desees con ellos)
        
    else:
        print(f"Error en la solicitud: {response.status_code}, {response.text}")
   
    nuevos_datosG = pd.DataFrame(data=data)
    if len(nuevos_datosG)==0:
        return 0
    else:
        nuevos_datosG.dropna(subset=['text'],inplace=True)

        nuevos_datosG.drop_duplicates(inplace=True)

        nuevos_datosG = nuevos_datosG.rename(columns={'reviewerId': 'user_id','placeId':'place_id'})

        # Convertir la columna publishedAtDate a tipo datetime
        nuevos_datosG['publishedAtDate'] = pd.to_datetime(nuevos_datosG['publishedAtDate'])

        # Extraer el mes, el año y la hora
        nuevos_datosG['month'] = nuevos_datosG['publishedAtDate'].dt.month
        nuevos_datosG['year'] = nuevos_datosG['publishedAtDate'].dt.year
        nuevos_datosG['hour'] = nuevos_datosG['publishedAtDate'].dt.hour

        nuevos_datosG.drop(columns=['publishedAtDate'],inplace=True)

        column_order = ['user_id', 'place_id', 'stars', 'text', 'month', 'year', 'hour']
        nuevos_datosG = nuevos_datosG[column_order]

        # We convert the text column to lowercase and remove the special characters

        def limpiar_texto(texto):
            # Check if the value is a string
            if isinstance(texto, str):
                # Convert to lowercase
                texto = texto.lower()
                # Remove special characters using regular expressions
                texto = re.sub(r'[^a-z0-9\s]', '', texto)
            return texto

        nuevos_datosG['text'] = nuevos_datosG['text'].apply(limpiar_texto)
        cliente_cloud_storage=storage.Client()
        ids=cliente_cloud_storage.bucket('ultabeauty2024').blob('businessId_gmapID.csv')
        businessId_gmapID=pd.read_csv(StringIO(ids))
        # Reemplazar 'Sin Valores' en 'place_id' con el valor de 'business_id'
        businessId_gmapID['place_id'] = businessId_gmapID.apply(lambda row: row['business_id'] if row['place_id'] == 'Sin Valores' else row['place_id'], axis=1)

        # Concatenar por la columna 'place_id'
        nuevos_datosG = pd.merge(businessId_gmapID, nuevos_datosG, on='place_id', how='inner')

        #Borramos la columna place_id
        nuevos_datosG.drop(columns=['Unnamed: 0','place_id'],inplace=True)
        nuevos_datosG['source']='G'
        return nuevos_datosG
def datos_nuevos_yelp():
    # URL de la API con el token
    api_url = "https://api.apify.com/v2/actor-tasks/elgabb01~yelp-scraper-task/runs/last/dataset/items?token=apify_api_RBIv1NkT8xL34eWjtg8UaSZsaWj6D02uuC8u&unwind=reviews"
    # Realiza la solicitud GET
    response = requests.get(api_url)

    # Verifica el estado de la respuesta
    if response.status_code == 200:
        # Parsea la respuesta JSON
        data = response.json()
    else:
        print(f"Error en la solicitud: {response.status_code}, {response.text}")
    df=pd.DataFrame(data)
    if len(df)==0:
        return 0
    else:
        df.drop(columns=['#error','#debug'],inplace=True)
        df['stars']=df['stars'].str.split(' ',expand=True)[0].astype('float64')
        df.rename(columns={'businessId':'business_id','userIDs':'user_id','fechas':'date','reviews':'text'},inplace=True)
        dfreviewsYelp=df
        #Obtenemos el año , mes y hora de la columna date

        dfreviewsYelp['date'] = pd.to_datetime(dfreviewsYelp['date'], format='%Y-%m-%d')
        dfreviewsYelp['month'] = dfreviewsYelp['date'].dt.month
        dfreviewsYelp['year'] = dfreviewsYelp['date'].dt.year
        dfreviewsYelp['hour'] = dfreviewsYelp['date'].dt.time
        dfreviewsYelp.drop(columns=['date'],inplace=True)

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
        return dfreviewsYelp

def writetobigquery(df,table_id):
    client_bigquery=bigquery.Client()
    job_config0 = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND')
    job = client_bigquery.load_table_from_dataframe(df, table_id, job_config=job_config0)

def query_bigquery(query,tabla,dataframe):
    client_bigquery=bigquery.Client()
    lista_business='('+str([a for a in dataframe['business_id'].unique()]).strip('[]')+')'
    query =f"SELECT {query} FROM `final-project-data-insght-pro.ultabeautyreviews.{tabla}` WHERE business_id in {lista_business}"
    if 'user_id' in dataframe.columns:
        lista_user='('+str([a for a in dataframe['user_id'].unique()]).strip('[]')+')'
        query+=f" AND user_id in {lista_user}"
    results=client_bigquery.query(f"""{query}""")
    return results

def checar_filas(dataframe_etl,tabla):
    dfquery=query_bigquery('*',tabla,dataframe_etl).result().to_dataframe()
    # Itera sobre cada fila de df2 y verifica si está contenida en df1
    filassincargar=[]
    for index, row in dataframe_etl.iterrows():
        is_contained = dfquery[dfquery.eq(row).all(axis=1)].shape[0] > 0
        if is_contained:
            continue
        else:
            filassincargar.append(row)
    df=pd.DataFrame(filassincargar)
    if len(filassincargar)==0:
        return 'datos ya registrados 2022-2023'
    else:
        writetobigquery(df,f'final-project-data-insght-pro.ultabeautyreviews.{tabla}')
        return 'carga de filas pendientes 2022-2023'
def cargar_logs(df,descripcion,archivo):
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hora= datetime.datetime.now().strftime("%H:%M:%S")
    lista=[{'Fecha':fecha,'Hora':hora,'Descripción':descripcion,'Archivo':archivo}]
    log=pd.DataFrame(lista)
    df=pd.concat([df,log])
    return df
def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    global credenciales
    # Crea una instancia del cliente de Google Cloud Storage
    client_storage = storage.Client()

    # Obtiene el bucket
    bucket = client_storage.get_bucket(bucket_name)

    # Crea un nuevo blob en el bucket utilizando el nombre de destino proporcionado
    blob = bucket.blob(destination_blob_name)

    # Sube el archivo al blob
    blob.upload_from_filename(source_file_path)

    print(f'Archivo {source_file_path} subido a {destination_blob_name} en el bucket {bucket_name}.')
def analyze_sentiment(df):
    def analysis(review):
        model = AutoModelForSequenceClassification.from_pretrained("Kaludi/Reviews-Sentiment-Analysis", use_auth_token=False)
        tokenizer = AutoTokenizer.from_pretrained("Kaludi/Reviews-Sentiment-Analysis", use_auth_token=False)
        
        inputs = tokenizer(review, return_tensors="pt")
        outputs = model(**inputs)
        predicted_label = torch.argmax(outputs.logits, dim=1).item()
        return predicted_label
    
    chunks=np.array_split(df,4)
    for ulta_beauty_chunk in chunks:      

        # Apply sentiment analysis to the 'text' column
        ulta_beauty_chunk['sentiment'] = ulta_beauty_chunk['text'].apply(lambda x: analysis(x))
        ulta_beauty_chunk['sentiment_label'] = ulta_beauty_chunk['sentiment'].apply(lambda x: 'positive' if x == 1 else 'negative')
        ulta_beauty_chunk['hour'] = ulta_beauty_chunk['hour'].astype(str).str.split(':').str[0].astype(int)
        writetobigquery(ulta_beauty_chunk, 'final-project-data-insght-pro.ultabeautyreviews.ulta_beauty_sentiment_analysis')
def run():
    client_storage = storage.Client()
    blob=client_storage.bucket('ultabeauty2024').blob('logs/logs_loads.csv')
    contenido = blob.download_as_text()
    logs=pd.read_csv(StringIO(contenido))
    if datos_nuevos_Google==0:
        pass
    else:
        df=datos_nuevos_Google()
        dfquery=query_bigquery('*','google_reviews_ulta_beauty',df).result().to_dataframe()
        # Itera sobre cada fila de df2 y verifica si está contenida en df1
        filassincargar=0
        for index, row in df.iterrows():
            is_contained = dfquery[dfquery.eq(row).all(axis=1)].shape[0] > 0
            if is_contained:
                continue
            else:
                filassincargar+=1
        if filassincargar==0:
            logs=cargar_logs(logs,'verificación de carga completa reviews google día anterior','google reviews')
        else:
            logs=cargar_logs(logs,'cargar las filas pendientes','google reviews')
        try:
            analyze_sentiment(df)
        except:
            logs=cargar_logs(logs,'error en analisis de sentimientos google','google reviews')
    if datos_nuevos_yelp==0:
        pass
    else:
        df=datos_nuevos_yelp()
        dfquery=query_bigquery('*','yelp_reviews_ulta_beauty',df).result().to_dataframe()
        # Itera sobre cada fila de df2 y verifica si está contenida en df1
        filassincargar=0
        for index, row in df.iterrows():
            is_contained = dfquery[dfquery.eq(row).all(axis=1)].shape[0] > 0
            if is_contained:
                continue
            else:
                filassincargar+=1
        if filassincargar>0:
            logs=cargar_logs(logs,'verificación de carga completa reviews yelp día anterior','yelp reviews')
        else:
            logs=cargar_logs(logs,'cargar las filas pendientes','yelp reviews')
        try:
            analyze_sentiment(df)
        except:
            logs=cargar_logs(logs,'error en analisis de sentimientos google','google reviews')
    try:

        logs.to_csv('logs_loads.csv',index=False)
        bucket_name = 'ultabeauty2024'
        source_file_path = 'logs_loads.csv'
        destination_blob_name = 'logs/logs_loads.csv'
        # Llama a la función para subir el archivo
        upload_to_gcs(bucket_name, source_file_path, destination_blob_name)
        del logs
    except Exception as e:
        print(f'error {e}')
    

   
if __name__ == '__main__':
    run()
