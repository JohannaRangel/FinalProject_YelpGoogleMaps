#import apache_beam as beam
#from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import os
from geopy.geocoders import Nominatim
import re
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import bigquery
import google.cloud.storage
from io import StringIO
import datetime

#crea servicio de google drive
def build_service():
    SCOPES= ['https://www.googleapis.com/auth/drive']
    service_account_file='service_account.json'
    creds=service_account.Credentials.from_service_account_file(service_account_file,scopes=SCOPES)
    service=build('drive','v3',credentials=creds)
    return service

#función para descargar archivos de google drive
def download_file(id,nombre):
    service=build_service()
    request = service.files().get_media(fileId=id)
    fh = open(nombre,'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done=False
    while done is False:
        status, done = downloader.next_chunk()
    # Cierra el archivo local
    fh.close()
#función para obtener la lista de archivos dentro de un folder 
def folders(id_folder):
    service=build_service()
    results = service.files().list(q=f"'{id_folder}' in parents",fields="files(id, name)").execute()
    files = results.get('files', [])
    return files
def cliente_bigquery():
    credentials_path='service_account.json'
    credentials =  service_account.Credentials.from_service_account_file(credentials_path, scopes = ['https://www.googleapis.com/auth/cloud-platform'])
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client
def writetobigquery(df,table_id):
    client=cliente_bigquery()
    job_config0 = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND')
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config0,)

def query_bigquery(query,tabla,dataframe):
    client=cliente_bigquery()
    lista_business='('+str([a for a in dataframe['business_id'].unique()]).strip('[]')+')'
    query =f"SELECT {query} FROM `windy-tiger-410421.UltaBeautyReviews.{tabla}` WHERE business_id in {lista_business}"
    if 'user_id' in dataframe.columns:
        lista_user='('+str([a for a in dataframe['user_id'].unique()]).strip('[]')+')'
        query+=f" AND user_id in {lista_user}"
    results=client.query(f"""{query}""")
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
    if filassincargar==0:
        return 'datos ya registrados'
    else:
        df=pd.DataFrame(filassincargar)
        writetobigquery(df,f'windy-tiger-410421.UltaBeautyReviews.{tabla}')
        return 'carga filas pendientes'
def validación(dataframe,tabla):
    results=query_bigquery('COUNT(*)',tabla,dataframe)
    registros=next(results.result())[0]
    largo_dataframe=len(dataframe)
    if registros==0:
        writetobigquery(dataframe,f'windy-tiger-410421.UltaBeautyReviews.{tabla}')
        return 'carga tabla completa'
    else:
        chequeo=checar_filas(dataframe,tabla)
        return chequeo
def cargar_logs(df,descripcion,archivo):
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hora= datetime.datetime.now().strftime("%H:%M:%S")
    lista=[{'Fecha':fecha,'Hora':hora,'Descripción':descripcion,'Archivo':archivo}]
    log=pd.DataFrame(lista)
    df=pd.concat([df,log])
    return df


def etl_business_yelp(dfbusinessYelp):
    
    #Filtrado de los datos para tener solamente ulta beauty 

    dfbusinessYelp = dfbusinessYelp.loc[:, ~dfbusinessYelp.columns.duplicated()]
    dfbusinessYelp = dfbusinessYelp[dfbusinessYelp['name'] == 'Ulta Beauty']

    #Borrar columnas irrelevantes para el proyecto

    columns_to_drop = ['is_open', 'address', 'name', 'categories','attributes','hours']
    dfbusinessYelp = dfbusinessYelp.drop(columns=columns_to_drop, axis=1)

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

    #Agregamos la columna source
    dfbusinessYelp['source']='Y'
    return dfbusinessYelp

def etl_reviews_yelp(dfreviewsYelp,ids):

    business_ids = ids
    """['4uqRhXZTOzKF2ZhxbWzxfA','fWMPbickerGWohPy2vDL5A','DJZQCN0NUej_EtviN4rUlg','Vxqa8u_5RD5e7oBqdaU0yQ','idP674ti6a8yg8z2xFcCgA',
                    'Vsx34Z-N5S5S0o0f2G6ORw','HLwzUJ8IZwP2mLpMMF5x7g','gNs9um_3hB3L8HFyMk2x4w','JGzrVEBaaVUd2VCsWlf47g','G4gzTUGV2xAoz6CG9STqfg','LivMZcvWxslI5G9At4K69w',
                    'CFyOrr--MWtxgjZvaHHV6Q','p-pp2F_iClEKk0wybXAaxg','n66mhHJpXftCUW9vXqeZwQ','CUi489qZNqfm2DsqL67BJw','FiIIuZGT-VJEju-bwNQTuA','9EZ6JKyqirjyE0GGHERmUQ',
                    'FPpm1z3z3SNEXrywKSnFBQ','LyQbIKnQN-3Pogi_5ZshNg','30rM6JGz_4cqjeBJZ93thA','Bb7-R7PdhknAWcl4GcLD7A','qDHInkhNalPHQi8rYi9kjA','qPohxPukFHiTsQ5wFQa0FQ',
                    'buLMYC9EsRiXU2anGU0PRg','6ZdgPekv8DEvwLmh66wJIQ','lAx1fYU_FMmpCesbHVK_Zg','xI3h9lvDlmVO9oH2_IAqUg','LAfBjmgtmIE0FqedMIyy1A','HvmnXUubTdxHK95SS0QiJg',
                    'IV6NVFYNeFEcWp8TEn5cwQ','3551sOADb1nQr3PFsbqw8A','W0PBojWcYIX0-DxOVKHfCw','b9bwm23B7_nOQ0UNW1NGkQ','EIMdpFSVbzITAn89Ha1eoA','iwSHtwr96TyhhnclQZHGEg',
                    'GUdFcEfoIywh-spJdsyN0g','HIE4JTORkwfuc_XO2nRVAQ','SCWi86ibAr-2_x8Ngd9rGg','z7N3_ZjkX8eXbutD1fvzJQ','XpjFyhS47TwCCbtGnINEXw','eMsW2f0JgCQ6-D6qJn1kWA',
                    'PxIrdyTFViCocKbIPN5zQA','YYJEGvjaEapprZnuFpW6tA','ndzDVdevAwhJFdXoc9lPCw','Z3JrRDH8jtPy2eyH4-Hx8A','ZsFOwyulu9PMDvA4dtBqsw','tGaPlSDHzEGHKnJkEdnDEQ',
                    'yqjA_Sd1c_aYPyaDyz3wbQ','Qf3FVkWVUFpVKwHaB6_iQg','hczzraTsDaGJNQV-fnK8hw','QM6CnjtMLVUyOvIj07XFAw','u_bN8-vC8D7MvkI8RakP4Q','E85lTnthikYTCxEsi39PWw',
                    'oJyhIkZY-0BkgtkmK7tRhg','TPUFZpI2RWEgJjOmLFtvCA','RMD5mNJgyQ1FGO_5YDCrzA','DLV4zM60EdyPFafEk88crg','THOXisAF58kiwXUv0h-w3g','652tRAf14Mu-2kzPKCeMbQ',
                    'TuMQjYCumnFhWJV2ELwwxQ','kZJs6j2VrWjNrtx_p2zpiw']"""
        
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
    return dfreviewsYelp

def etl_business_google(dfbusinessGoogle):
    
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

    return dfbusinessGoogle

def etl_reviews_google(dfreviewsGoogle):
    business_ids_to_keep = ['0x80e849691015d7b7:0x314b8627656bc6d5','0x89e7ad0b7da12a11:0x2cab11e09a406d3a','0x89c2d130d433cc0b:0xb550e43a7ce3540',
                            '0x89c30355aba7b017:0x3735266fb77aee42','0x89b62b6664208fed:0x5c970eed3a4cec49','0x89c39bff14375f07:0xc5ee9c1210c05e9a',
                            '0x54c8d5e692d7ae15:0x29cf0300c7eb68ce','0x89e8bfb8c1c6ea03:0x26dba039722009e0','0x88c2c39fe4d578a9:0x6189d55364509b43',
                            '0x88626770c6f52645:0x509b4f1c55e02490','0x89e4bc81e70c1451:0x718ba1be3e35898a','0x87f43a7a4e647ce7:0xb990c473b8e85463',
                            '0x80c34bbe0919c561:0x4b279e03dbdd8fc1','0x549f88262755026d:0x85c10dc33c648934','0x89c1879ce42ea40d:0xdb5908f8d4ef0886',
                            '0x88388f323eb6aaab:0x57fb8bd31aca0a82','0x8632d528f6d437a5:0x3c2b0e8f2d86c3e2','0x5495728b86a037fb:0xb530bc240a174a3c',
                            '0x54994740f3e4a15d:0x194a0b8f4559b15f','0x87befdda22a09025:0x7c34e6ca036879be','0x8830935bad81a517:0x1606fb68c0dd24ca',
                            '0x89c6cbbebd9789bf:0xddfe5a5440d73af2','0x880fc7f4d19ff7c7:0x1888c3028bac4771','0x80dcf16f6a8bbe25:0x192172bd44b90da6',
                            '0x89ca8f885447fff3:0x99efea0b7c49c86f','0x89e848fe64ce4f83:0x74857e8e3984210d','0x80daf727d197ac05:0xf7ac9fd1d54c3df4',
                            '0x89e653bf31c5f9ab:0x9659e4ac4e64721c','0x88d92f3897c94b4f:0xc0655cbea7e2cc30','0x56c8942452d84811:0xe5c4b11a9df014e6',
                            '0x89e5aeb4781327db:0x39f11aa7bed613ee','0x880eea4e2e69579f:0x71585725d57ea47a','0x89c3d42532ca8baf:0xa69d15d499919f34',
                            '0x88002162a3492b83:0x8d1498305df2658e','0x872a6ff6df7e31db:0x702113bb22fc19c2','0x88ff678fdf87b951:0xa7828dd7c00a62e5',
                            '0x88f58533492249c1:0x5b0cbf923ce6a0d6','0x89e389817ec44871:0x8c98a39f95381fcf','0x88672370ce97f43f:0xc831d995840f6fa3',
                            '0x89d0825a0a93a329:0x76b52d4eed64ccca','0x80dce086039a433d:0xf2df2336b87f04','0x88db6b507476f9e9:0x711c0932b482976d',
                            '0x8620a6feea733329:0xabee5aac871b2d8b','0x864e9e619b3ac561:0x4f9fe23013c83b66','0x88f5dd4f760652e5:0x3bd180900f870bb3',
                            '0x8085b3f896717a4f:0x9e7ab78286d90e8f','0x873b8f313c152dcf:0xa4e7da38659f09d8','0x88dc5795efd9f2fd:0x2e2c67a58a1106c7',
                            '0x87d7b08a7494478d:0x8f21900684e1078','0x80946f18448695c1:0xa4e8fb865009e976','0x808577a7ce822e91:0xc2a94739b232f0e0',
                            '0x89c80f0537d1342d:0x9a845d4e7c0060ee','0x80c2d980af465d05:0xb3658f28ee3c13dc','0x89c6d1d73166c45d:0x80da2844ced834a5',
                            '0x88388e6da42a9513:0xa7e890efceacf7f3','0x87220ddb59291a7d:0xc8183901d89e27c6','0x88d9dd92328f010d:0xb8c03e0083f3fcd8',
                            '0x874dbd1f6fd52e81:0xda6420bf418465be','0x872b0fa5c59dc94d:0x177bac3a8f9dde4b','0x865c672f46a46013:0x8a305de85b893ade',
                            '0x87b68bb35d59b4e1:0x9ef0e703a52d6f22','0x8816e9916be9a2f7:0x9ee6eefd086f8612','0x89c133080894d907:0x457d462952082336',
                            '0x864e725c48ca5cd9:0xa1719b85ecceecbf','0x880e2ca358cc8e5d:0x9dc20359b5bb3630','0x8875477ba6ca7da3:0xe52278f8cc3be314',
                            '0x80dceb07824add5b:0xb6de8fc24de211d6','0x863ab52c13d72fbb:0xd441a3bed94961f5','0x8805428428e55f55:0x597ad5743f2d948a',
                            '0x87afd258ceccdb0d:0x182166c80c81a863','0x87b71391ecd5d699:0x8218ac1e560c6043','0x87d9492d89038ec1:0x645544d2d0fa257e',
                            '0x88585f469083041f:0xc21bf48f12f9fc4c','0x880fbb0e296fc935:0xb789d2ad3e6b7eb9','0x89e3f3251fe76bd9:0x38a75d0576e1a0cd',
                            '0x87e2366e08f9588d:0xb74d7a6fe82aa2b1','0x88357be1549bf96f:0xff8d427f62837d0','0x876c81e4f574cbcf:0x72cdbb2bc721c72f',
                            '0x863b867b2ce90771:0xdd9d8ccec3e1f72c','0x89b7e0f7b514ee9d:0xd3b2e10bd04103b4','0x549072b0aa9fcc05:0xeb7610b6f18b7e7a',
                            '0x87a4b63ea209eaff:0x889839977d1ffdcf','0x8880705d48ecc4f1:0xbadb0cbec189263e','0x880fcf41d1d9cdbf:0x9213629971dd64cf',
                            '0x80851426a3c6d431:0xe91f3ce49ee521d3','0x883cf171b7e300a9:0x9c30c698b0293104','0x89e2f14311b99549:0x82623797038346a0',
                            '0x5361c0e762a750ff:0x967cdde1ae163b6d','0x89e7d0cbb5191eff:0x9333bb1d98029a8','0x808e2e2285fae447:0x8b13689e10b3900a',
                            '0x883462a63d203419:0x74ea4b6a6d9e2c09','0x87c0dd32c7634ab7:0xa3e74a017bc11cff','0x80c8d6b410317733:0x89d78bd26d34e5d6',
                            '0x88db17100f390105:0xd395811e26dc0021','0x89d6b17e57f4d7d7:0xa69b86a6b78554da','0x8886ebd7a692619f:0x3f867a0058002100',
                            '0x88388a627d2e64b5:0xf34c3de8abec9f22','0x87b21a1785ab517f:0xee41ce35aee31845','0x865b37ebc039a31b:0x8e1796e3a818fefe',
                            '0x88e72777f7c69b2d:0x8e549f07a41a22e1','0x88fae3f774351653:0xfcfef03ac4a56fa','0x8856817640c7338d:0x3f868f37e393ad8e',
                            '0x80dcef4bbc4915a1:0x56c658364bc1040e','0x88f8b004d3b876b9:0x3e1f087b5b19a157','0x87e4f10976e97cb3:0xff9aa4a164343111',
                            '0x864e2183b2b97d1f:0x2358817be298ed97','0x52b3236eaf105175:0x8393439a89fcfab0','0x864e890b254d8a15:0x79b34c6510a2643',
                            '0x880e29718ddc36e1:0xbaf4e05bd1172021','0x8854b48a4fb234e9:0x9905f1bbdb285b3e','0x89e85b0462c40323:0x58489e65d001255',
                            '0x549041677a79caab:0xff3c68fef5b197b3','0x808f91ce0bd2e4bb:0xa2aabf6c8d3c3c35','0x864c33f817b7e87b:0x257c4497264f8d93',
                            '0x80e82eb5a58ef6af:0x3dc0fc9cee8800fd','0x883093a060f7b667:0x7752359bf2238c48','0x8836d6ca1199fcd3:0xd173ea92aed2fd57',
                            '0x87547de81cc5a38d:0x146f73d65868b467','0x8810c436c08c5c45:0x82fcff088f4bca05','0x87cf6146434a1f5f:0xa4c9814860360520',
                            '0x89c30966eac2b4dd:0x4832071b66534f0b','0x889383e7afd4e7b9:0x10c5f384fd54ae41','0x80859a189e64bd75:0xff714649a523c287',
                            '0x88c31798c9632c9b:0x164fbff71aa67507','0x864c1bdd6fa74417:0x6d65852e35886667','0x864c217be6712961:0x125f822be582f448',
                            '0x89baeb17a20a61ff:0x5d91452e3e698ce4','0x89c2f9fd1572d30b:0xc681734d9e5cc1ae','0x864c2f758f5ce065:0x32a9c6c522fc0e2f',
                            '0x89c2fb7923cf1bc1:0x44473272f4eea9b','0x80db43a819c3406d:0xcd744bd5111dd265','0x89e82bf6140baeed:0x8129aa8c05fac584',
                            '0x89e6d16cb7a055d1:0xd4c12cb7d2dea352','0x8626b5c18a472f51:0xf76988396c99cad','0x8640c7cb4370df9b:0x7b80fe395f38c50e',
                            '0x880e57b87a85e763:0x820b50c20d04778','0x8745627826b205a7:0xa51ac3ac6c281b6a','0x88847d011708b257:0x7106d32a68da553c',
                            '0x7c0015742be7c589:0xa6f432ddffe2b5e2','0x880f99997809f01d:0x23c8080052b667f1', '0x80c291baa2d8114f:0x37cf09934731a395',
                            '0x876c6c4493d4fc1f:0xda60de8fd444ca75','0x80ec1f120e132d8b:0xc17e88fa1dc68b3b','0x864043ea8cb92a89:0xfa986a13fa787ea6',
                            '0x80ea43726e77319b:0x55459e94d1a5cc4e','0x88892218e9cd6499:0x421281dcaf386e1d','0x80c29d92076f0621:0x3c28efe120cad609',
                            '0x89d375ed738685a1:0xf94796ad47550bf0','0x8644d7378e955b4d:0x332ab1014d32203a','0x89c259f5cd286f9f:0xd7f304c78a07bf1d',
                            '0x88e8bc9c1cb8a9c9:0xc54af76cc3aa026e','0x89c22f83fd079ae7:0xee3529b535dd9bd7','0x89c2f590aa8b7d31:0x924a1bbe0afca374',
                            '0x880fc7af7a4e4e75:0x642cff114e6f4196','0x89e7ca02be0dbcf9:0x6ed41a5b33f76942','0x80dd2b9281580c83:0x35d93e9b1861b999',
                            '0x80c285098a6816c5:0x24ddd9b0f48acba0','0x8846778196af8c1f:0xc43ccd2a2c7d31ec','0x89fb3123be2d306b:0xca5797c60119d611',
                            '0x89e6e517a916a47b:0x8e88519f1e5b56e','0x8888a833c3af13fd:0x343c3567897354e5','0x864c1d93a69f734b:0x6049a29eefbff700',
                            '0x89aa750d6d06b215:0xf9cdaa06e632d545','0x864c11610741c9d3:0xa6082d6a86127e27','0x89c8475a263725cf:0x4cd5cc2241ecad27',
                            '0x88d9b49ad5a22fd9:0x1972e5c0120e5216','0x88f8aa49f6a6da93:0xf5337c4e9e42e4b8','0x89e3632d0b64029f:0xf8db66ec73c70f09',
                            '0x80e940afbdae32e1:0xcd1f0ccb65b20372','0x8640dd16e03afb8d:0xe9bd757533133347','0x89de09afb465d365:0x21bc75937afa9f90',
                            '0x52b45f0f2ed93f99:0x94a56b99e6c52723','0x88e69d5ecda6858f:0x8789c8088c2ac29d','0x889385f0f0a13739:0xd1b66ba01811dce5',
                            '0x89a9f5ab2e82540d:0xf69a95cec7025765','0x864e794e3801c82f:0x8702030325ab5c28','0x88d9c791124ae941:0xc52faa7aa12527b3',
                            '0x87e444732f66fd47:0x8937a7df838de1cb','0x54900dfa59fa734f:0x7dd6b1c56e0c5d9','0x89c3e17a94f96725:0x307c9fb4c397f981',
                            '0x864c88eec6fc7f0f:0xb4e228a6cb419601','0x89de30c8a8f69883:0xe6e09c879eed0716','0x883884f2d74bacb5:0x6e75affcc84a72ac',
                            '0x80c29fcfdcdff34d:0x3a056477869b22d8','0x88fa47413d0e09b1:0x316e320f01b3a53a','0x88f54f776e158bf3:0x9bc6edd9c227f86c',
                            '0x87df37e603946bed:0x9ec830f9cba03cb7','0x8805a5e7df5597d7:0x308dadda55a555b9','0x80db1c92a60009b7:0x3e6fcabf0c041398',
                            '0x880fbb08b30978c1:0xf78f3c2da1d5ca90','0x89c6a5d33ba94bdb:0x4985e8d4b7cbd2da','0x874d9afa7e46e697:0xaf5ba3db378a2e4c',
                            '0x80dce8fa0359425d:0x46aab3dcd793594e','0x88f3e13747cfd28d:0x4cd6dbd2fb5371aa','0x86409900c7cb7d8d:0x3b1bab3daf6ff21e',
                            '0x89c6c7b40610d7eb:0x849f3b8b3b380a53','0x87b26b79d8dfe8e7:0xd5fe9c6f735cde3b','0x80857e13ff1e5f43:0xde648abea14b40a8',
                            '0x888a0323e016f07d:0x2cc24d39a9db25c1','0x89e7c103b44769df:0x654ac80919fd6c09','0x864e6d2fd9580217:0xf02238785342323d',
                            '0x89c2866d1deb70bf:0xb5bf19c6cfe49c41','0x89b7c96e814a6a5b:0x370df137084f7bf2','0x8817ee6fc7fba571:0xcb2a36eac5257f38',
                            '0x87471d3a23a94051:0x5d32d92f5446718b','0x80c2cc19697ba4cf:0x8d8cf9c32a11dad4','0x863f59569ede4915:0xae09e05f286c703b',
                            '0x8890c011574c2c31:0x2f2089b26c7aa75d','0x808de493b5644441:0x18e17ea68cebea9e','0x8626bf6d42ff525b:0xf3a4a802a107a0c6',
                            '0x88582b439b10c11b:0x4830bbc944c714de','0x89c61d3da7817d7f:0x99158e228819ae01','0x533495e641aa6e71:0xcb788f90b8c056ae',
                            '0x8824a22f7134e217:0x5c3c45d1846df803','0x88169eaf42edb957:0x9bcd07671f44bd49','0x880f2a3599e32235:0x78d70e95b48bca4c',
                            '0x886075331e6ff26d:0xbe86a29a0102542b','0x878eb391e3bc762f:0x93a75472f6356722','0x4cca796c462e7503:0xf6b405e6de72a08f',
                            '0x4cb203d7a345bdeb:0x3c5b1e7bb59a7e9e','0x888925f3a96abce1:0x4be2f1d433222891','0x864c394f020aaa49:0x20e59a02a019a45d',
                            '0x876c766470d23375:0x81706d5842938edb','0x5342381c6ddf30e3:0xfbe922695b89d6de','0x872b16a68b8e62d1:0x7d7cf67a7c53d8a3',
                            '0x808fe985ad5d01df:0x20e8c372f20b006c','0x88d91958f9f6f4ab:0x111e84f49744687d','0x880fa30ab6b747f3:0x2a88bf8178a2c164',
                            '0x89b7b873f4bb3653:0x2279dc47461cd2d8','0x89b7633b0309cef7:0x3be7662a577800f0','0x89e45d7b5d17823b:0x633fc817a20dc197',
                            '0x808f86a51b871b89:0xb32edb857537adcf','0x89c99ef4abfbecc7:0xc711aac2e48817ae','0x89e88aac23f7f23f:0x49ab9cf0a87c873',
                            '0x80dd4b91504c62b1:0xb3cc9dca79aa311d','0x883ef3a61fd8283b:0x8e94dd5107d09edc','0x87b6f1e3d067ff7d:0x11d955f4a804eb54',
                            '0x87df2abea993cd4f:0xf937b38a41d537bc','0x80dd2e536f430ea9:0xb56fc5d02329aa45','0x89e6778725d32203:0xe5f89de21172ad5e',
                            '0x89c4727e09899fd1:0x5e9a64ab3765ec2','0x89e405bbeed21f7f:0xfa1bae7dae7677d2','0x89acf761a874832d:0x111803f8757a81e0']

    #business_ids_to_keep=ids
    dfreviewsGoogle = dfreviewsGoogle[dfreviewsGoogle['gmap_id'].isin(business_ids_to_keep)]

    #Borrar columnas inecesarias

    dfreviewsGoogle.drop(columns=['name','pics','resp'],inplace=True)

    #Extraer el mes , año , hora

    dfreviewsGoogle['time'] = pd.to_datetime(dfreviewsGoogle['time'], unit='ms')
    dfreviewsGoogle['month'] = dfreviewsGoogle['time'].dt.month
    dfreviewsGoogle['year'] = dfreviewsGoogle['time'].dt.year
    dfreviewsGoogle['hour'] = dfreviewsGoogle['time'].dt.hour
    dfreviewsGoogle.drop(columns=['time'],inplace=True)

    # Filtrar las filas para los años 2019, 2020 y 2021
    dfreviewsGoogle = dfreviewsGoogle[(dfreviewsGoogle['year'] == 2019) | (dfreviewsGoogle['year'] == 2020) | (dfreviewsGoogle['year'] == 2021)]

    #Renombrar las columnas
    dfreviewsGoogle = dfreviewsGoogle.rename(columns={'gmap_id': 'business_id', 'rating': 'stars'})

    # Reordenar las columnas
    column_order = ['user_id', 'business_id', 'stars', 'text', 'month', 'year', 'hour']
    dfreviewsGoogle = dfreviewsGoogle[column_order]

    # Eliminar filas donde la columna 'text' es nula
    dfreviewsGoogle = dfreviewsGoogle.dropna(subset=['text'])

    # Eliminar filas duplicadas en función de todas las columnas
    dfreviewsGoogle = dfreviewsGoogle.drop_duplicates()

    #La columna texto la convertimos a toda minuscula y quitamos caracteres especiales
    def limpiar_texto(texto):
        if isinstance(texto, str):
            texto = texto.lower()
            texto = re.sub(r'[^a-z0-9\s]', '', texto)
        return texto

    dfreviewsGoogle['text'] = dfreviewsGoogle['text'].apply(limpiar_texto)

    #Agregamos la columna source 
    dfreviewsGoogle['source']='G'
    dfreviewsGoogle['user_id'] = dfreviewsGoogle['user_id'].apply(lambda x: f'{x:.0f}')

    return dfreviewsGoogle
from google.cloud import storage

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    # Crea una instancia del cliente de Google Cloud Storage
    client = storage.Client.from_service_account_json('service_account.json')

    # Obtiene el bucket
    bucket = client.get_bucket(bucket_name)

    # Crea un nuevo blob en el bucket utilizando el nombre de destino proporcionado
    blob = bucket.blob(destination_blob_name)

    # Sube el archivo al blob
    blob.upload_from_filename(source_file_path)

    print(f'Archivo {source_file_path} subido a {destination_blob_name} en el bucket {bucket_name}.')

# Configura estos valores con tu información específica



def run():
    client = google.cloud.storage.Client.from_service_account_json('service_account.json')
    blob=client.bucket('ultabeauty').blob('logs/logs_loads.csv')
    contenido = blob.download_as_text()

    logs=pd.read_csv(StringIO(contenido))
    """try: #ETL business Yelp
    # Descarga archivo business.pkl
        if os.path.exists('business_yelp.pkl'):
            pass
        else:
            download_file('1byFtzpZXopdCN-XYmMHMpZqzgAqfQBBu', 'business_yelp.pkl')
        print('descarga completa business_yelp.pkl')
        df_business_yelp=pd.read_pickle('business_yelp.pkl')
        print('lectura comopleta  business_yelp.pkl')
        df_business_yelp=etl_business_yelp(df_business_yelp)
        print('etl completo business_yelp.pkl')
        validación_business_yelp=validación(df_business_yelp,'yelp_business_data')
        if validación_business_yelp=='datos ya registrados':
            logs=cargar_logs(logs,validación_business_yelp,'buisness_yelp.pkl')
            print('validación completa')
        if validación_business_yelp=='carga filas pendientes' or validación_business_yelp=='carga tabla completa':
            logs=cargar_logs(logs,validación_business_yelp,'buisness_yelp.pkl')
            print('validación completa')
        
    except Exception as e:
        logs=cargar_logs(logs,f'error {e}','dataset yelp business')
        print(f'error {e}')
    os.remove('business_yelp.pkl')
    ids_yelp=df_business_yelp['business_id'].unique()
    del df_business_yelp

    #ETL reviews Yelp
    #descargamos el archivo en local
    try:
        if os.path.exists('reviews_yelp.parquet'):
            pass
        else:
            download_file('1C2-ZAOIiUoh8FtpvIFINTa6Rb0ev5CAy','reviews_yelp.parquet')
        dfreviewsyelp=pd.read_parquet('reviews_yelp.parquet')
        dfreviewsyelp=etl_reviews_yelp(dfreviewsyelp,ids_yelp)
        validación_reviews_yelp=validación(dfreviewsyelp,'yelp_reviews_ulta_beauty')
        if validación_reviews_yelp=='datos ya registrados':
            logs=cargar_logs(logs,validación_reviews_yelp,'reviews_yelp.parquet')
        if validación_reviews_yelp=='carga filas pendientes' or validación_reviews_yelp=='carga tabla completa':
            logs=cargar_logs(logs,validación_reviews_yelp,'reviews_yelp.parquet')
        del dfreviewsyelp
    except Exception as e:
        logs=cargar_logs(logs,f'error {e}','dataset yelp reviews')
        print(f'error {e}')

    # Borra el archivo descargado
    os.remove('reviews_yelp.parquet')

    #ETL metadata business Google
    try:
        list_ids=folders('1olnuKLjT8W2QnCUUwh8uDuTTKVZyxQ0Z')
        no_archivos=len(list_ids)
        for b,a in enumerate(list_ids):
            try:
                
                if os.path.exists(a['name']):
                    pass
                else:
                    download_file(a['id'],'metadata_google'+a['name'])
                print(f'descarga completa archivo no. {b+1} de {no_archivos}')
                dfbusinessGoogle=etl_business_google(pd.read_json('metadata_google'+a['name'],lines=True))
                validación_business_Google=validación(dfbusinessGoogle,'google_business_data')
                print(f'archivo no. {b+1} de {no_archivos}')
                if validación_business_Google=='datos ya registrados':
                    logs=cargar_logs(logs,validación_business_Google,'metadata_google'+a['name'])
                    continue
                if validación_business_Google=='carga filas pendientes'or validación_business_Google=='carga tabla completa':
                    logs=cargar_logs(logs,validación_business_Google,'metadata_google'+a['name'])
                os.remove('metadata_google'+a['name'])
                del validación_business_Google
                del dfbusinessGoogle
            except:
                name=a['name']
                print(f'error en carga de archivo metadata Google{name}')
        print('carga de metadata Google completa')
    except Exception as e:
        logs=cargar_logs(logs,f'error {e}','dataset metadata Google')
        print(f'error {e}')"""
    #ETL reviews Google"""
        
    #descargamos cada carpeta por estado, hacemos el ETL de cada carpeta y guardamos en bigquery
    try:
        folders_list=folders('19QNXr_BcqekFNFNYlKd0kcTXJ0Zg7lI6')
        for a in folders_list:
            folders_list2=folders(a['id'])
            for c,b in enumerate(folders_list2):
                download_file(b['id'],a['name'].split('-')[1]+str(c)+'.json')
                dfreviewsGoogle=pd.read_json(a['name'].split('-')[1]+str(c)+'.json',lines=True)
                dfreviewsGoogle=etl_reviews_google(dfreviewsGoogle)
                if len(dfreviewsGoogle)==0:
                    os.remove(a['name'].split('-')[1]+str(c)+'.json')
                    continue
                validación_reviews_Google=validación(dfreviewsGoogle,'google_reviews_ulta_beauty')
                if validación_reviews_Google=='datos ya registrados':
                    logs=cargar_logs(logs,validación_reviews_Google,'reviews_google'+a['name']+str(c)+'.json')
                    del validación_reviews_Google
                    continue
                if validación_reviews_Google=='carga filas pendientes' or validación_reviews_Google=='carga tabla completa':
                    logs=cargar_logs(logs,validación_reviews_Google,'reviews_google_'+a['name']+str(c)+'.json')
                    del validación_reviews_Google
                del dfreviewsGoogle
                os.remove(a['name'].split('-')[1]+str(c)+'.json')
    except Exception as e:
        logs=cargar_logs(logs,f'error {e}','dataset metadata google')
        print(f'error {e}')
    try:

        logs.to_csv('logs_loads.csv',index=False)
        bucket_name = 'ultabeauty'
        source_file_path = 'logs_loads.csv'
        destination_blob_name = 'logs/logs_loads.csv'
        # Llama a la función para subir el archivo
        upload_to_gcs(bucket_name, source_file_path, destination_blob_name)
        del logs
    except Exception as e:
        print(f'error {e}')
   
if __name__ == '__main__':
    run()