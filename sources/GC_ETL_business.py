#Importar librerias
import pandas as pd
import os

#Lectura del archivo

jsons=os.listdir(r"D:\Datasets_proyecto\metadata-sitios")
for a in jsons:
    ruta=f'D:\Datasets_proyecto\metadata-sitios\\{a}'
    dfbusinessGoogle=pd.read_json(ruta,lines=True)

#Filtramos los datos relacionados con ulta beauty

dfbusinessGoogle = dfbusinessGoogle[dfbusinessGoogle['name'] == 'Ulta Beauty']


#Filtramos los negocios que se encuentran permanentemente cerrados

dfbusinessGoogle = dfbusinessGoogle[dfbusinessGoogle['state'] != 'Permanently closed']
dfbusinessGoogle = dfbusinessGoogle.drop('state', axis=1)


#Obtenemos la ciudad, estado y codigo postal de la direccion

dfbusinessGoogle['address'] = dfbusinessGoogle['address'].str.replace('Ulta Beauty,', '').str.strip()
dfbusinessGoogle['city'] = dfbusinessGoogle['address'].str.split(',').str[1]
dfbusinessGoogle['state_PostalCode'] = dfbusinessGoogle['address'].str.split(',').str[2]
dfbusinessGoogle['state']=dfbusinessGoogle['state_PostalCode'].str.split(' ').str[1]
dfbusinessGoogle['postal_code']=dfbusinessGoogle['state_PostalCode'].str.split(' ').str[2]



#Borramos las columnas irrelevantes
columnas_a_eliminar = ['name', 'address', 'state_PostalCode', 'description', 'category', 'price', 'relative_results', 'url', 'MISC', 'hours']
dfbusinessGoogle = dfbusinessGoogle.drop(columns=columnas_a_eliminar, axis=1)


# Eliminar duplicados 
dfbusinessGoogle = dfbusinessGoogle.drop_duplicates()

#Renombrar las columnas 
dfbusinessGoogle = dfbusinessGoogle.rename(columns={'gmap_id': 'business_id', 'avg_rating': 'stars', 'num_of_reviews': 'review_count'})

#Cambiamos los nombres de las ciudades
dfbusinessGoogle['city'] = dfbusinessGoogle['city'].str.strip().str.lower().str.replace(' ', '_')

#Cambiamos las siglas de los estados
estado_mapping = {'TX': 'texas', 'MT': 'montana', 'CA': 'california', 'FL': 'florida',
                  'IL': 'illinois', 'MD': 'maryland', 'MA': 'massachusetts', 'PA': 'pennsylvania',
                  'NY': 'new_york', 'OH': 'ohio', 'OK': 'oklahoma', 'MO': 'missouri',
                  'CT': 'connecticut', 'NJ': 'new_jersey', 'NC': 'north_carolina'}

# Reemplazar las siglas por los nombres en la columna 'state_postalCode'
dfbusinessGoogle['state'] = dfbusinessGoogle['state'].replace(estado_mapping)

#Cambiar el orden de las columnas 
nuevo_orden_columnas = ['business_id', 'city', 'state', 'postal_code', 'latitude', 'longitude', 'stars', 'review_count']
dfbusinessGoogle = dfbusinessGoogle[nuevo_orden_columnas]

#Agregar la columna source
dfbusinessGoogle['source']="G"



