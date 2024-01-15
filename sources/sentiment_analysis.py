import pandas as pd
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from google.cloud import bigquery

'''conexión a big query'''

# Configura tu proyecto y credenciales
project_id = 'windy-tiger-410421'
client = bigquery.Client(project=project_id)

# Especifica tu conjunto de datos y tabla
dataset_id = 'UltaBeautyReviews'
table_idG = 'google_reviews_ulta_beauty'
table_idY = 'yelp_reviews_ulta_beauty'

# Función para convertir una fila de BigQuery a un DataFrame de Pandas
def convertir_a_dataframe(row):
    return row[0]

# Realiza la consulta a BigQuery para Google Reviews
queryG = f"SELECT * FROM `{project_id}.{dataset_id}.{table_idG}` LIMIT 5"
query_jobG = client.query(queryG)
G_ulta_beauty = query_jobG.result()

# Convierte los resultados a un DataFrame de Pandas
df_G_ulta_beauty = pd.DataFrame([convertir_a_dataframe(row) for row in G_ulta_beauty], columns=list(G_ulta_beauty.schema.keys()))

# Repite el proceso para Yelp Reviews
queryY = f"SELECT * FROM `{project_id}.{dataset_id}.{table_idY}` LIMIT 5"
query_jobY = client.query(queryY)
Y_ulta_beauty = query_jobY.result()
df_Y_ulta_beauty = pd.DataFrame([convertir_a_dataframe(row) for row in Y_ulta_beauty], columns=list(Y_ulta_beauty.schema.keys()))

# Concatena los DataFrames si es necesario
ulta_beauty = pd.concat([df_G_ulta_beauty, df_Y_ulta_beauty], ignore_index=True)

# Imprime el DataFrame resultante
print(ulta_beauty.head())
print('Los datos se cargaron correctamente')

'''Correr el modelo'''

# Drop rows where 'text' is not a valid string
ulta_beauty = ulta_beauty.dropna(subset=['text'])
ulta_beauty = ulta_beauty[ulta_beauty['text'].apply(lambda x: isinstance(x, str))]

# Load the pre-trained model and tokenizer
model = AutoModelForSequenceClassification.from_pretrained("Kaludi/Reviews-Sentiment-Analysis", use_auth_token=False)
tokenizer = AutoTokenizer.from_pretrained("Kaludi/Reviews-Sentiment-Analysis", use_auth_token=False)

# Function to apply the model to each review
def analyze_sentiment(review):
    inputs = tokenizer(review, return_tensors="pt")
    outputs = model(**inputs)
    predicted_label = torch.argmax(outputs.logits, dim=1).item()
    return predicted_label

# Apply the function to the 'text' column of ulta_beauty
ulta_beauty['sentiment'] = ulta_beauty['text'].apply(lambda x: analyze_sentiment(x))

print(ulta_beauty['sentiment'])
print('El modelo se entreno correctamente')

'''Cargar resultados



ulta_beauty.to_csv('..\datasets\csv\sentiment_analysis.csv')'''