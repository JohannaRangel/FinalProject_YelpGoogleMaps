import pandas as pd
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from google.cloud import bigquery
from google.oauth2 import service_account

'''conexión a big query'''

# Configura tu proyecto y credenciales
project_id = 'windy-tiger-410421'
client = bigquery.Client(project=project_id)

# Especifica tu conjunto de datos y tabla para Yelp Reviews
dataset_id = 'UltaBeautyReviews'
table_idY = 'yelp_reviews_ulta_beauty'
table_idG = 'google_reviews_ulta_beauty'

# Obtiene el esquema de la tabla de Yelp Reviews y Google Reviews
tableY = client.get_table(f'{project_id}.{dataset_id}.{table_idY}')
tableG = client.get_table(f'{project_id}.{dataset_id}.{table_idG}')

# Construye y ejecuta la consulta para Yelp Reviews
queryY = f"SELECT * FROM `{project_id}.{dataset_id}.{table_idY}`"
query_jobY = client.query(queryY)
df_Y_ulta_beauty=query_jobY.to_dataframe()

# Construye y ejecuta la consulta para Google Reviews
queryG = f"SELECT * FROM `{project_id}.{dataset_id}.{table_idG}`"
query_jobG = client.query(queryG)
df_G_ulta_beauty=query_jobG.to_dataframe()

ulta_beauty = pd.concat([df_G_ulta_beauty, df_Y_ulta_beauty], ignore_index=True)
print ('Datos leidos correctamente')

print(ulta_beauty.head())


'''Funciones'''

# Drop rows where 'text' is not a valid string
ulta_beauty = ulta_beauty.dropna(subset=['text'])
ulta_beauty = ulta_beauty[ulta_beauty['text'].apply(lambda x: isinstance(x, str))]


#Función para etiquetar el sentimiento y tokenizar el texto y
def analyze_sentiment(review):
    print('tokenizando el texto')
    model = AutoModelForSequenceClassification.from_pretrained("Kaludi/Reviews-Sentiment-Analysis", use_auth_token=False)
    tokenizer = AutoTokenizer.from_pretrained("Kaludi/Reviews-Sentiment-Analysis", use_auth_token=False)
    print('Analizando el sentimiento')
    inputs = tokenizer(review, return_tensors="pt")
    outputs = model(**inputs)
    predicted_label = torch.argmax(outputs.logits, dim=1).item()
    return predicted_label


def writetobigquery(df,table_id):
    client=cliente_bigquery()
    job_config0 = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND',create_disposition= 'CREATE_IF_NEEDED')
    client.load_table_from_dataframe(df, table_id, job_config=job_config0,)

def cliente_bigquery():
    credentials_path='service_account.json'
    credentials =  service_account.Credentials.from_service_account_file(credentials_path, scopes = ['https://www.googleapis.com/auth/cloud-platform'])
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

# Dividir el dataframe en partes para facilitar la carga

num_parts = 20
total_rows = len(ulta_beauty)
chunk_size = total_rows // num_parts

for i in range(18,num_parts):
    start_idx = i * chunk_size
    end_idx = (i + 1) * chunk_size if i != num_parts - 1 else total_rows
    
    # Leer cada parte
    ulta_beauty_chunk = ulta_beauty.loc[start_idx:end_idx-1, :]

    # Apply sentiment analysis to the 'text' column
    ulta_beauty_chunk['sentiment'] = ulta_beauty_chunk['text'].apply(lambda x: analyze_sentiment(x))
    ulta_beauty_chunk['sentiment_label'] = ulta_beauty_chunk['sentiment'].apply(lambda x: 'positive' if x == 1 else 'negative')
    ulta_beauty_chunk['hour'] = ulta_beauty_chunk['hour'].astype(str).str.split(':').str[0].astype(int)

    # Write the chunk to BigQuery
    print('Cargando a BigQuery')
    writetobigquery(ulta_beauty_chunk, 'windy-tiger-410421.UltaBeautyReviews.ulta_beauty_sentiment_analysis')

    print(f'parte {i+1}/{num_parts} cargada a BigQuery exitosamente')

print('Carga completa')

