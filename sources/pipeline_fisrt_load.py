import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import gdown
import os

def download_file(id,llave):
    url='https://drive.google.com/uc?id='+id
    gdown.download(url, output=llave)

def download_folder(nombre,id):
    gdown.download_folder(id=id,output=nombre)

def leer_archivo(ruta):
    if '.pkl':
        return pd.read_pickle(ruta)
    if '.json':
        return pd.read_json(ruta)
# Función de limpieza y transformación de datos con Pandas

def clean_and_transform(df):
    df=df
    # Realiza las operaciones de limpieza y transformación aquí
    # Por ejemplo, df = df.dropna()
    return df
def etl_reviews_yelp(df):

    return df

def cargar_bigquery(df,table):
    credentials_path='windy-tiger-410421-956bf231305a.json'
    project_id = 'windy-tiger-410421'
    credentials =  service_account.Credentials.from_service_account_file(credentials_path, scopes = ['https://www.googleapis.com/auth/cloud-platform'])
    # Crear un cliente de BigQuery utilizando las credenciales
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    job_config0 = bigquery.LoadJobConfig(write_disposition = 'WRITE_TRUNCATE')
    job = client.load_table_from_dataframe(df, table_id= 'windy-tiger-410421.prueba1.'+table, job_config=job_config0)

def run():
    #descargamos archivo business.pkl de Yelp a nuestro directorio loca, donde estamos actualmente
    download_file('1byFtzpZXopdCN-XYmMHMpZqzgAqfQBBu','business_yelp.pkl')

    #pasamos a un datafrme el archivo business_yelp.pkl
    dfbusinessYelp=pd.read_pickle('business_yelp.pkl')

    #aqui va el ETL business_yelp.pkl
    dfbusinessYelp=etl_business(dfbusinessYelp)
    #cargamos a bigquery
    cargar_bigquery(dfbusinessYelp,'tabla business')
    
    #borramos erl archivo descragado
    os.remove('business_yelp.pkl')

    #seleccionamos las columna de los ids de Ulta Beauty
    id_Ulta_Beauty=dfbusinessYelp['id_business'].unique().tolist()
    
    del dfbusinessYelp

    #ETL reviews Yelp

    #descargamos el archivo reviews yelp al directorio local
    download_file('1byFtzpZXopdCN-XYmMHMpZqzgAqfQBBu','review_yelp.json')
    #usaremos chunks para procesar este archivo, debido al peso que este tiene, para no tener fallos en la memoria
    chunk_size = 1000

    # Lee el archivo en chunks
    chunks = pd.read_json('review_yelp.json', lines=True, chunksize=chunk_size)

    # Procesa cada chunk por separado
    for chunk in chunks:
        dfreviewsYelp=etl_reviews_yelp(chunk)
        cargar_bigquery(dfreviewsYelp,'reviews yelp')
        del dfreviewsYelp
    os.remove('review_yelp.json')

    #ETL metadata-sitios google

    #descargamos los archivos al directorio local
    download_folder('1olnuKLjT8W2QnCUUwh8uDuTTKVZyxQ0Z','metadata_google')

    #leemos los archivos y los procesamos uno a uno los json que contiene cada archivo
    jsons=os.listdir(r'metadata_google')
    for a in jsons:
        ruta=f'metadata_google\\{a}'
        dfbusinesGoogle=pd.read_json(ruta,lines=True)

    # hacemos las transformaciones paa cada json de metadata de google
        dfbusinessGoogle=ETL(dfbusinessGoogle)

    #cargamos a bigquery
        cargar_bigquery(dfbusinessGoogle,'negocios google')
    
    #borramos el dataset de la memoria
    del dfbusinessGoogle

    #borramos la carpeta de los jsons
    os.remove('metadata_google')

    #ETl Reviews Google
    #descargamos cada carpeta por estado, hacemos el ETL de cada carpeta y guardamos en bigquery
    folders_id={'Alabama':'1d8qkSNSvIioGxQGywN0GE_HDIqYB2ZZv','Alaska':'1pgC5X_XObV0g4-Mli3J7eoglzhNNJa5o','Arizona':'1-w20axtXbCNNai7jc4NYyDJf7vW7q2wV',
            'Arkansas':'1TgjCDBc0gwDdFIOdxEr2K__Pw8lJVRsV','California':'1Jrbjt-0hnLCvecfrnMwGu1jYZSxElJll','Colorado':'1IlUZJZxOyRiIWo3G6BqW1Thu2kXYKMFX',
            'Connecticut':'1xZEGYWvQMLtWUfgo7TV9BdT_n7GNG3F1','Delaware':'1Dpo2hIQBbgUsCl4_EJjYTCc2Y8DUWORT',
            'District_of_Columbia':'1MeJcfRmuNfp-xhSd2iIIiCojePR5Ss0u','Florida':'1kxYcx3BjWNR2IVJ9odksaPRVOWzwQJts',
            'Georgia':'1MuPznes6CebS6gyWPVU-kR4EVKKLY4l3','Hawaii':'1IIAsDSlBiqQEdoN_n0E8a6TTAnrJMyWf','Idaho':'1QsYqI9xyTkyy3XqMGOQO1Ry5IibwQp1b',
            'Illinois':'1F8x0ymIgQInUCaqAxdkG89h2IQesNSEM','Indiana':'1QRr2lTZpIHzTEdc8xAzsOP1Na-9nmu94','Iowa':'1VgoUBVISFDHjlXJ4nRlAGPQkWFLleH9N',
            'Kansas':'1301Fcj19UfYg2175WzAgOy9TT-iERPkf','Kentucky':'1phkjuHo7nYWvA3uDPdj3J3O3-4qqzwUD','Louisiana':'1fxJVlbj2Z0ZBoOoHqgMBlH_1nIhwycg2',
            'Maine':'1E3eJcycrHEQKJB7K1JKBEqj8dlYvSapa','Maryland':'1lDIIS19yyECxmDtiB8Mqxaz1c-pinOHM','Massachusetts':'1ORrUfBkvwJ4PiyxovgvWT2f1G9_y5r6c',
            'Michigan':'1up0B5BBxIXuwB0Ue6gl1KpM6iLPpC6Wz','Minnesota':'1mozfz7mswyjoQejGtctV0ipTIhuFnCL7','Mississippi':'1i5zhK-rlDtsUYmGW7dGIMsPTMwhXaXxJ',
            'Missouri':'1NrCFwbGlQz6i5w-QHvL9r8VENgtXMMgo','Montana':'1-YytZf6hIimIWMfcgk7QBXGT8PlEnJft','Nebraska':'1sGd0TxRR2MujzQUXftQFg6PLrPRmwsHi',
            'Nevada':'1W8x6jX1u0fCvpSf0hPHK5rg5jftKEjXK','New_Hampshire':'1NTDhffJyPQ-o067yB2OEz92lYCkFLatK','New_Jersey':'1jktC8qBJqIOBFf8ZNF7hmB66LHcqlvlG',
            'New_Mexico':'1n11VoHrVved8T4Ppdt4ri4cB3-_QgY05','New_York':'18HYLDXcKg-cC1CT9vkRUCgea04cNpV33',
            'North_Carolina':'16D1dwKtCbWDJGhr_k9nsZomRjAqff8np','North_Dakota':'1jXCgzHcUU6cpVIYxEb-piFezrWcPERSa','Ohio':'1zwKir_Cih_Nh7nW_XcP244VQghBroGZd',
            'Oklahoma':'146v-ZHRNQBFdGaB-mDspyyloLg3JO_Fj','Oregon':'1yLp_EEKQRfpmuDQjHSy24Es7_oRqk_jv','Pennsylvania':'1S6WHaD2uXzEzl6w7aQT7oknQqbvfEw1j',
            'Rhode_Island':'1UCUBqlJsEJQzYCr1hoGOTlD1iPyPf1Qj','South_Carolina':'11Vv61BJwZKbGb8FXQOvQQ88y7b3VZX4i',
            'South_Dakota':'1wG-pyem8sdj94NqO7fuSgX8wGxQWzwFU','Tennessee':'1m8GV0m4geI_i6Bfe9YVGjIOVFoTzSOLJ','Texas':'1zq12pojMW2zeGgts0lHFSf_pF1L_4UWr',
            'Utah':'1ll2VZUERIaAQyngnfiF0VSsg2WqxQqWN','Vermont':'1RF-zTyWPPi1DWHVYR4LSqyknFMAcxuo3','Virginia':'1OV8FeeuiYHmIIpMVitliFO_w4XLK1pXi',
            'Washington':'1y6MqAZNUmOW8zXArm_-UEq38Ej-0vp7a','West-Virginia':'1ffuT8ch4UPQ2Hz71TNGC3bKlfkfCv1cy',
            'Wisconsin':'1KednQoExNw-pq9uZGLxNEdVEV3NRJXJX','Wyoming':'1XBl_mEvdaSt2K_4TOebIArrm4smnv-im'}
    for a in folders_id.keys():
        download_folder(folders_id[a])
        

#def run(url): #,table_id,client
    #download(url)
    #df=leer_json('business.pkl')
    #clean_and_transform(df)
    #print(df.head())
    #cargar_bigquery(df,table_id,client)

if __name__ == '__main__':
    #run(url,table_id,client)
    run(url)