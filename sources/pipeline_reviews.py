import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import re
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import bigquery

#función para descargar los datos de los negocios de Yelp
def download_file(id,nombre):
    SCOPES= ['https://www.googleapis.com/auth/drive']
    service_account_file='service_account.json'
    creds=service_account.Credentials.from_service_account_file(service_account_file,scopes=SCOPES)
    service=build('drive','v3',credentials=creds)
    request = service.files().get_media(fileId=id)
    fh = open(nombre,'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done=False
    while done is False:
        status, done = downloader.next_chunk()
    # Cierra el archivo local
    fh.close()

def folders(id_folder):
    SCOPES= ['https://www.googleapis.com/auth/drive']
    service_account_file='service_account.json'
    creds=service_account.Credentials.from_service_account_file(service_account_file,scopes=SCOPES)
    service=build('drive','v3',credentials=creds)
    results = service.files().list(q=f"'{id_folder}' in parents",
                                    fields="files(id, name)").execute()
    files = results.get('files', [])
    return files

#funcion para descargar un folder con archvios de google drive
def ids_folder(folder_id):
    SCOPES= ['https://www.googleapis.com/auth/drive']
    service_account_file='service_account.json'
    creds=service_account.Credentials.from_service_account_file(service_account_file,scopes=SCOPES)
    service=build('drive','v3',credentials=creds)
    ids=service.files().list(
        q=f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder'",
        pageSize=1000,  # Puedes ajustar este valor según sea necesario
        fields="files(id)"
        ).execute()['files']
    ids=[a['id'] for a in ids]
    return ids

def writetobigquery(df,table_id):
    credentials_path='service_account.json'
    credentials =  service_account.Credentials.from_service_account_file(credentials_path, scopes = ['https://www.googleapis.com/auth/cloud-platform'])
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    job_config0 = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND')
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config0)

def etl_reviews_yelp(dfreviewsYelp):

    business_ids = ['4uqRhXZTOzKF2ZhxbWzxfA','fWMPbickerGWohPy2vDL5A','DJZQCN0NUej_EtviN4rUlg','Vxqa8u_5RD5e7oBqdaU0yQ','idP674ti6a8yg8z2xFcCgA',
                    'Vsx34Z-N5S5S0o0f2G6ORw','HLwzUJ8IZwP2mLpMMF5x7g','gNs9um_3hB3L8HFyMk2x4w','JGzrVEBaaVUd2VCsWlf47g','G4gzTUGV2xAoz6CG9STqfg','LivMZcvWxslI5G9At4K69w',
                    'CFyOrr--MWtxgjZvaHHV6Q','p-pp2F_iClEKk0wybXAaxg','n66mhHJpXftCUW9vXqeZwQ','CUi489qZNqfm2DsqL67BJw','FiIIuZGT-VJEju-bwNQTuA','9EZ6JKyqirjyE0GGHERmUQ',
                    'FPpm1z3z3SNEXrywKSnFBQ','LyQbIKnQN-3Pogi_5ZshNg','30rM6JGz_4cqjeBJZ93thA','Bb7-R7PdhknAWcl4GcLD7A','qDHInkhNalPHQi8rYi9kjA','qPohxPukFHiTsQ5wFQa0FQ',
                    'buLMYC9EsRiXU2anGU0PRg','6ZdgPekv8DEvwLmh66wJIQ','lAx1fYU_FMmpCesbHVK_Zg','xI3h9lvDlmVO9oH2_IAqUg','LAfBjmgtmIE0FqedMIyy1A','HvmnXUubTdxHK95SS0QiJg',
                    'IV6NVFYNeFEcWp8TEn5cwQ','3551sOADb1nQr3PFsbqw8A','W0PBojWcYIX0-DxOVKHfCw','b9bwm23B7_nOQ0UNW1NGkQ','EIMdpFSVbzITAn89Ha1eoA','iwSHtwr96TyhhnclQZHGEg',
                    'GUdFcEfoIywh-spJdsyN0g','HIE4JTORkwfuc_XO2nRVAQ','SCWi86ibAr-2_x8Ngd9rGg','z7N3_ZjkX8eXbutD1fvzJQ','XpjFyhS47TwCCbtGnINEXw','eMsW2f0JgCQ6-D6qJn1kWA',
                    'PxIrdyTFViCocKbIPN5zQA','YYJEGvjaEapprZnuFpW6tA','ndzDVdevAwhJFdXoc9lPCw','Z3JrRDH8jtPy2eyH4-Hx8A','ZsFOwyulu9PMDvA4dtBqsw','tGaPlSDHzEGHKnJkEdnDEQ',
                    'yqjA_Sd1c_aYPyaDyz3wbQ','Qf3FVkWVUFpVKwHaB6_iQg','hczzraTsDaGJNQV-fnK8hw','QM6CnjtMLVUyOvIj07XFAw','u_bN8-vC8D7MvkI8RakP4Q','E85lTnthikYTCxEsi39PWw',
                    'oJyhIkZY-0BkgtkmK7tRhg','TPUFZpI2RWEgJjOmLFtvCA','RMD5mNJgyQ1FGO_5YDCrzA','DLV4zM60EdyPFafEk88crg','THOXisAF58kiwXUv0h-w3g','652tRAf14Mu-2kzPKCeMbQ',
                    'TuMQjYCumnFhWJV2ELwwxQ','kZJs6j2VrWjNrtx_p2zpiw']
        
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
    writetobigquery(dfreviewsYelp,'windy-tiger-410421.UltaBeautyReviews.yelp_reviews_ulta_beauty')
    del dfreviewsYelp

def run():
    options=PipelineOptions(
        runner='DataflowRunner',  #' Ejemplo: especificando el runner
        project='windy-tiger-410421',
        custom_gcs_temp_location='gs://ultabeauty/tmp',
        region='us-central1',)
        # Crea y configura el pipeline de Apache Beam
    with beam.Pipeline(options=options) as p:


        download_file('1byFtzpZXopdCN-XYmMHMpZqzgAqfQBBu','reviews_yelp.json')

        import pandas as pd

        file_path = 'reviews_yelp.json'
        chunk_size = 2000  # ajusta el tamaño del fragmento según tus necesidades

        # Iterar sobre el archivo en bloques más pequeños
        chunks = pd.read_json(file_path, lines=True, chunksize=chunk_size, encoding='utf-8')

        # Procesar cada fragmento o realizar las operaciones que necesitas
        for chunk in chunks:
            etl_reviews_yelp(chunk)

if __name__ == '__main__':
    run()
