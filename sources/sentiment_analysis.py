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
table_idG = 'yelp_reviews_ulta_beauty'

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
print ('Datos cargados correctamente')

print(ulta_beauty.head())


'''Correr el modelo'''

# Drop rows where 'text' is not a valid string
ulta_beauty = ulta_beauty.dropna(subset=['text'])
ulta_beauty = ulta_beauty[ulta_beauty['text'].apply(lambda x: isinstance(x, str))]

#Tokenizar la columna de texto
model = AutoModelForSequenceClassification.from_pretrained("Kaludi/Reviews-Sentiment-Analysis", use_auth_token=False)
tokenizer = AutoTokenizer.from_pretrained("Kaludi/Reviews-Sentiment-Analysis", use_auth_token=False)

#Función para etiquetar el sentimiento
def analyze_sentiment(review):
    inputs = tokenizer(review, return_tensors="pt")
    outputs = model(**inputs)
    predicted_label = torch.argmax(outputs.logits, dim=1).item()
    return predicted_label

# Apply the function to the 'text' column of ulta_beauty
ulta_beauty['sentiment'] = ulta_beauty['text'].apply(lambda x: analyze_sentiment(x))
ulta_beauty['sentiment_label'] = ulta_beauty['sentiment'].apply(lambda x: 'positive' if x == 1 else 'negative')

print('El modelo se entreno correctamente')

'''Cargar resultados'''

def writetobigquery(df,table_id):
    client=cliente_bigquery()
    job_config0 = bigquery.LoadJobConfig(write_disposition = 'WRITE_TRUNCATE',create_disposition= 'CREATE_IF_NEEDED')
    client.load_table_from_dataframe(df, table_id, job_config=job_config0,)

def cliente_bigquery():
    credentials_path='service_account.json'
    credentials =  service_account.Credentials.from_service_account_file(credentials_path, scopes = ['https://www.googleapis.com/auth/cloud-platform'])
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client

writetobigquery(ulta_beauty,'windy-tiger-410421.UltaBeautyReviews.ulta_beauty_sentiment_analysis')

print('La carga a BigQuery se cargo de manera exitosa')

