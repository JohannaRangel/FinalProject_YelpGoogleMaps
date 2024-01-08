import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import io

url = 'https://drive.google.com/uc?id=1_itlZQHrvaUlcCbJs-iY9Rar9aovnDxs'
credentials_path='c:\\Users\\p2_ge\\Downloads\\windy-tiger-410421-956bf231305a.json'
project_id = 'windy-tiger-410421'
table_id = 'windy-tiger-410421.prueba1.tabla2'
credentials =  service_account.Credentials.from_service_account_file(credentials_path, scopes = ['https://www.googleapis.com/auth/cloud-platform'])
# Crear un cliente de BigQuery utilizando las credenciales
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

def cargar_dataset(file_url):
    response = requests.get(file_url)
    # Verificar si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
    # Cargar el contenido en un DataFrame de Pandas
        df = pd.read_csv(io.BytesIO(response.content))
        return df
    else:
        print(f'Error al obtener el archivo. Código de estado: {response.status_code}')

# Función de limpieza y transformación de datos con Pandas
def clean_and_transform(df):
    df=df
    # Realiza las operaciones de limpieza y transformación aquí
    # Por ejemplo, df = df.dropna()
    return df

def cargar_bigquery(df,table_id,client):

    job_config0 = bigquery.LoadJobConfig(write_disposition = 'WRITE_TRUNCATE')

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config0)

def run(url,table_id,client):
    df=cargar_dataset(url)
    clean_and_transform(df)
    cargar_bigquery(df,table_id,client)


if __name__ == '__main__':
    run(url,table_id,client)
