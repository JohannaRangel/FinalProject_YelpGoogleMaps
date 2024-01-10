import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import gdown
import os
#función para descargar los datos de los negocios de Yelp
def download_file(id,nombre):
    url='https://drive.google.com/uc?id='+id
    gdown.download(url, output=nombre)
#funcion para descargar un folder con archvios de google drive
def download_folder(id,nombre):
    gdown.download_folder(id=id,output=nombre)

def etl_business_yelp(df):
    
    return df

def etl_reviews_yelp(df):
    #coigo etl
    return df

def etl_business_google(df):
    #codigo etl
    return df

def etl_reviews_google(df):
    #codigo etl
    return df

class PandasTransform(beam.DoFn):
    def process(self, element,funcion_etl):
        # Aplica tu lógica de transformación con Pandas aquí
        df = pd.DataFrame([element])
        
        # Realiza el ETL con Pandas
        df_result = funcion_etl(df)
class ProcessFileDoFn(beam.DoFn):
    def process(self, element, etl_function):
        file_path = element  # La ruta del archivo local
        df = pd.read_json(file_path, lines=True)
        df_result = etl_function(df)
        yield df_result.to_dict(orient='records')

        # Convierte el DataFrame resultante a una lista de diccionarios
        yield df_result.to_dict(orient='records')
def writetobigquery(pcoll_transformed,nombre_tabla,options):
    pcoll_transformed| 'Write to BigQuery' >> WriteToBigQuery(
            table='windy-tiger-410421.UltaBeautyReviews.'+nombre_tabla,
            dataset='UltaBeautyReviews',
            project='windy-tiger-410421',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            method='STREAMING_INSERTS',  # Puedes ajustar el método de escritura según tus necesidades
            insert_retry_strategy='RETRY_NEVER',  # Ajusta la estrategia de reintento según tus necesidades
            gcs_location=options.temp_location,
        )


def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='windy-tiger-410421',
        #creaer un bucket temporal antes de ejecutar
        temp_location='gs://mi-bucket-temporal/tmp'
    )
    # Crea y configura el pipeline de Apache Beam
    with beam.Pipeline(options=options) as p:

        #ETL business Yelp
        # Descarga archivo business.pkl
        download_file('1byFtzpZXopdCN-XYmMHMpZqzgAqfQBBu', 'business_yelp.pkl')
        df_business_yelp=pd.read_pickle('business_yelp.pkl')

        # Lee el archivo en un PCollection
        pcoll_business_yelp = p | 'Read Yelp Business Data' >> beam.io.ReadFromPandas(df_business_yelp)
        pcoll_business_yelp_transformed = (
            pcoll_business_yelp
            | 'Apply Pandas Transformation' >> beam.ParDo(PandasTransform(etl_business_yelp))
        )
        writetobigquery(pcoll_business_yelp_transformed,'yelp_business_data',options)

        # Borra el archivo descargado
        os.remove('business_yelp.pkl')

        #ETL reviews Yelp
        #descargamos el archivo en local
        download_file('1byFtzpZXopdCN-XYmMHMpZqzgAqfQBBu','reviews_yelp.json')

        # Lee el archivo en un PCollection
        pcoll_business_yelp = p | 'Read Yelp Business Data' >> beam.io.ReadFromText('reviews_yelp.json')
        
        pcoll_business_yelp_transformed = (
            pcoll_business_yelp
            | 'Apply Pandas Transformation' >> beam.ParDo(PandasTransform(etl_reviews_yelp))
        )

        # Escribir a BigQuery
        writetobigquery(pcoll_business_yelp_transformed, 'yelp_reviews_ulta_beauty',options)

        # Borra el archivo descargado
        os.remove('business_yelp.json')

        #ETL metadata business Google

        download_folder('1olnuKLjT8W2QnCUUwh8uDuTTKVZyxQ0Z','metadata_google')
         
        # Dentro del bloque 'with beam.Pipeline(options=options) as p:'
        local_input_path = 'metadata_google'
        pcoll_business_google = (
            p
            | 'List Local Files' >> beam.Create(os.listdir(local_input_path))
            | 'Get Local File Path' >> beam.Map(lambda file_name: os.path.join(local_input_path, file_name))
            | 'Read and Process Local Files' >> beam.ParDo(ProcessFileDoFn(), etl_function=etl_business_google)
        )
        writetobigquery(pcoll_business_google,'google_business_data',options)
        os.remove('google_business_data')

        #ETL reviews Google

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

            download_folder(folders_id[a],a)
            
            # Dentro del bloque 'with beam.Pipeline(options=options) as p:'
            local_input_path = a
            pcoll_reviews_google = (
                p
                | 'List Local Files' >> beam.Create(os.listdir(local_input_path))
                | 'Get Local File Path' >> beam.Map(lambda file_name: os.path.join(local_input_path, file_name))
                | 'Read and Process Local Files' >> beam.ParDo(ProcessFileDoFn(), etl_function=etl_reviews_google)
            )
            writetobigquery(pcoll_reviews_google,'google_reviews_ulta_beauty',options)
            os.remove(a)

if __name__ == '__main__':
    run()
