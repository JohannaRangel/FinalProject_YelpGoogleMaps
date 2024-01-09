#Importar librerias
import pandas as pd
import requests
import io
import re
from datetime import datetime
from geopy.geocoders import Nominatim

#Lectura del archivo 

def cargar_dataset_pickle(file_url):
    response = requests.get(file_url)
    # Verificar si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
    # Cargar el contenido en un DataFrame de Pandas
        df = pd.read_pickle(io.BytesIO(response.content))
        return df
    else:
        print(f'Error al obtener el archivo. Código de estado: {response.status_code}')

url='https://drive.usercontent.google.com/download?id=1byFtzpZXopdCN-XYmMHMpZqzgAqfQBBu&export=download&authuser=0&confirm=t&uuid=189d48ef-ef64-4bc9-aec7-9e654cb4c757&at=APZUnTVmlezTicVw58BDGG9siM6Q:1704808512976'
dfbusinessYelp=cargar_dataset_pickle(url)

#Filtrado de los datos para tener solamente ulta beauty 

dfbusinessYelp = dfbusinessYelp.loc[:, ~dfbusinessYelp.columns.duplicated()]
dfbusinessYelp = dfbusinessYelp[dfbusinessYelp['name'] == 'Ulta Beauty']

#Borrar columnas irrelevantes para el proyecto

columns_to_drop = ['is_open', 'address', 'name', 'categories','BusinessParking','RestaurantsPriceRange2']
dfbusinessYelp = dfbusinessYelp.drop(columns=columns_to_drop, axis=1)

# Expandir el diccionario en la columna 'attributes'
def expandir_attributes(row):
    if row['attributes']:
        return pd.Series(row['attributes'])
    else:
        return pd.Series()
df_expandido = dfbusinessYelp.apply(expandir_attributes, axis=1)
df_resultado = pd.concat([dfbusinessYelp, df_expandido], axis=1)
dfbusinessYelp = df_resultado.drop('attributes', axis=1)

# Expandir el diccionario en la columna 'hour'
def expandir_hour(row):
    if row['hours']:
        hour_dict = row['hours']
        expanded_dict = {}
        for day, time_range in hour_dict.items():
            expanded_dict[f'hours_{day}'] = time_range
        return pd.Series(expanded_dict)
    else:
        return pd.Series()

df_expandido_hour = dfbusinessYelp.apply(expandir_hour, axis=1)
df_resultado = pd.concat([dfbusinessYelp, df_expandido_hour], axis=1)
dfbusinessYelp = df_resultado.drop('hours', axis=1)

#Quitamos columnas con muchos nulos 
dfbusinessYelp.drop(columns=['DogsAllowed','WheelchairAccessible','WiFi','GoodForKids'],inplace=True)

#Rellenamos los nulos
dfbusinessYelp[['BikeParking', 'BusinessAcceptsCreditCards', 'ByAppointmentOnly']] = dfbusinessYelp[['BikeParking', 'BusinessAcceptsCreditCards', 'ByAppointmentOnly']].fillna('False')

#Cambio de nombre de las columnas
def snake_case(column_name):
    return re.sub(r'(?<=[a-z])(?=[A-Z])', '_', column_name).lower()
dfbusinessYelp.columns = dfbusinessYelp.columns.map(snake_case)

#Normalización de los nombres en las filas
dfbusinessYelp['city'] = dfbusinessYelp['city'].str.lower().str.replace(' ', '_')
estado_mapping = {
    'PA': 'pennsylvania',
    'FL': 'florida',
    'NV': 'nevada',
    'LA': 'louisiana',
    'AZ': 'arizona',
    'IN': 'indiana',
    'TN': 'tennessee',
    'MO': 'missouri',
    'CA': 'california',
    'ID': 'idaho',
    'NJ': 'new_jersey',
    'DE': 'delaware',
    'IL': 'illinois',
    'AB':'AB'
}
dfbusinessYelp['state'] = dfbusinessYelp['state'].map(estado_mapping)


#Correción del estado AB
def obtener_estado(latitud, longitud):
    geolocator = Nominatim(user_agent="my_geocoder")
    location = geolocator.reverse((latitud, longitud), language="en")

    if location is not None:
        # La información sobre el estado generalmente se encuentra en el nivel de "address" en la respuesta
        estado = location.raw.get('address', {}).get('state', None)
        return estado
    else:
        return None
sindato=0
faltantes=dfbusinessYelp[dfbusinessYelp.state=='AB'][['latitude', 'longitude']].drop_duplicates()
for a in range(len(faltantes)):
    latitud=faltantes.iloc[a,0]
    longitud=faltantes.iloc[a,1]

    estado = obtener_estado(latitud, longitud)

    if estado:
        dfbusinessYelp.loc[(dfbusinessYelp.latitude==latitud)&(dfbusinessYelp.longitude==longitud),'state']=estado
    else:
        sindato+=1
        continue

#Normalización de la columna horas 
def convertir_formato_horas(rango_horas):
    hora_inicio, hora_fin = map(lambda x: datetime.strptime(x, "%H:%M"), rango_horas.split('-'))
    return f"{hora_inicio.strftime('%H:%M')} - {hora_fin.strftime('%H:%M')}"
columnas_dias_semana = [col for col in dfbusinessYelp.columns if col.startswith('hours_')]
for columna in columnas_dias_semana:
    dfbusinessYelp[columna] = dfbusinessYelp[columna].apply(convertir_formato_horas)

#Agregamos la columna source
dfbusinessYelp['source']='Y'


