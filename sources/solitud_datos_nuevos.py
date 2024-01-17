import json
import requests
from datetime import datetime, timedelta

# Datos en formato JSON que deseas enviar
# URL de tu API
api_url = "https://api.apify.com/v2/actor-tasks/elgabb01~google-maps-scraper-task/runs?token=apify_api_RBIv1NkT8xL34eWjtg8UaSZsaWj6D02uuC8u"
fecha= datetime.now() - timedelta(days=1)
fecha=datetime.strftime(fecha,'%Y-%m-%d')
# Datos en formato JSON que deseas enviar
data_to_send = {
  "deeperCityScrape": False,
  "includeWebResults": False,
  "language": "en",
  "maxCrawledPlacesPerSearch": 1,
  "maxImages": 0,
  "maxReviews": 99999,
  "onlyDataFromSearchPage": False,
  "reviewsSort": "newest",
  "reviewsStartDate": f"{fecha}",
  "scrapeDirectories": False,
  "scrapeResponseFromOwnerText": True,
  "scrapeReviewId": False,
  "scrapeReviewUrl": False,
  "scrapeReviewerId": True,
  "scrapeReviewerName": True,
  "scrapeReviewerUrl": False,
  "skipClosedPlaces": False,
  "startUrls": [
    {
      "requestsFromUrl": "https://apify-uploads-prod.s3.us-east-1.amazonaws.com/Qp1zGPjdnjFDdIDip-G_establishments_url.txt"
    }
  ]
}

# Convierte el diccionario a formato JSON
json_data = json.dumps(data_to_send)

# Configura las cabeceras
headers = {
    "Content-Type": "application/json"
}

# Envia la solicitud POST con los datos JSON
# response = requests.post(api_url, data=json_data, headers=headers)
response = requests.post(api_url, data=json_data, headers=headers)


# Verifica el estado de la respuesta
if response.status_code == 200:
    print("Solicitud exitosa!")
else:
    print(f"Error en la solicitud: {response.status_code}, {response.text}")

#query YElp



# URL de tu API
api_url = "https://api.apify.com/v2/actor-tasks/elgabb01~yelp-scraper-task/runs?token=apify_api_RBIv1NkT8xL34eWjtg8UaSZsaWj6D02uuC8u"

# Datos en formato JSON que deseas enviar
data_to_send = {
  "breakpointLocation": "NONE",
  "browserLog": False,
  "debugLog": False,
  "downloadCss": True,
  "downloadMedia": False,
  "ignoreCorsAndCsp": False,
  "ignoreSslErrors": False,
  "injectJQuery": True,
  "injectUnderscore": False,
  "keepUrlFragments": False,
  "maxRequestRetries": 2,
  "pageFunction": '',
  "proxyConfiguration": {
    "useApifyProxy": True
  },
  "runMode": "DEVELOPMENT",
  "startUrls": [
    {
      "requestsFromUrl": "https://apify-uploads-prod.s3.us-east-1.amazonaws.com/vFTijjrNui4FrG2Tc-Y_establishments_url.txt"
    }
  ],
  "useChrome": False,
  "useStealth": False,
  "waitUntil": [
    "networkidle2"
  ]
}
a="async function pageFunction(context) {\n    const $ = context.jQuery;\n\n    const businessIdElement = $('meta[name=\"yelp-biz-id\"]');\n    const businessId = businessIdElement.length > 0 ? businessIdElement.attr('content') : \"No se encontró el elemento <meta> con el atributo name='yelp-biz-id'\";\n\n    const diccionario= {\n        userIDs: [],\n        fechas: [],\n        stars: [],\n        reviews: [],\n    };\n\n    // Extraccion de user_id\n    $('div.user-passport-info.css-1qn0b6x').each(function() {\n        const aElement = $(this).find('a.css-19v1rkv');\n        if (aElement.length > 0) {\n            const userID = aElement.attr('href').match(/userid=([^&]+)/)[1];\n            diccionario.userIDs.push(userID);\n        }\n    });\n\n    // Extraccion de fecha\n    $('div.css-10n911v span.css-chan6m').each(function() {\n    const fechaString = $(this).text().trim();\n\n    // Convertir la fecha al formato \"YYYY-MM-DD\"\n    const fechaParts = fechaString.split(' ');\n    const monthAbbreviation = fechaParts[0];\n    const day = fechaParts[1].replace(',', '');\n    const year = fechaParts[2];\n\n    // Mapear las abreviaturas de meses a números de mes\n    const monthNames = {\n        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,\n        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12\n    };\n    const month = monthNames[monthAbbreviation];\n\n    const formattedDate = `${year}-${month.toString().padStart(2, '0')}-${day.padStart(2, '0')}`;\n    diccionario.fechas.push(formattedDate);\n    });\n\n\n    // Extraccion de stars\n    $('div.css-10n911v div.css-14g69b3').each(function() {\n        const ariaLabel = $(this).attr('aria-label');\n        diccionario.stars.push(ariaLabel);\n    });\n\n    // Extraccion de Review\n    $('p.comment__09f24__D0cxf.css-qgunke').each(function() {\n        const spanElement = $(this).find('span.raw__09f24__T4Ezm');\n        if (spanElement.length > 0) {\n            const commentText = spanElement.text().trim();\n            diccionario.reviews.push(commentText);\n        } else {\n            diccionario.reviews.push(Null);\n        }\n    });\n\n\n    //Filtramos el diccionario por fecha\n    \n    // Filtrar las filas con fecha mayor a 2021-01-01\n    const fechaLimite = Date.parse(\""
fecha= datetime.now() - timedelta(days=1)
fecha=datetime.strftime(fecha,'%Y-%m-%d')
b="\");\n\n    const diccionarioFiltrado = {\n        userIDs: diccionario.userIDs.filter((_, index) => Date.parse(diccionario.fechas[index]) > fechaLimite),\n        fechas: diccionario.fechas.filter((fecha) => Date.parse(fecha) > fechaLimite),\n        stars: diccionario.stars.filter((_, index) => Date.parse(diccionario.fechas[index]) > fechaLimite),\n        reviews: diccionario.reviews.filter((_, index) => Date.parse(diccionario.fechas[index]) > fechaLimite),\n    };\n\n\n\n\n    // Trasformamos a una lista de diccionarios\n    const lista = [];\n\n    // Suponiendo que todas las listas en el diccionario tienen la misma longitud\n    const longitud = diccionarioFiltrado.userIDs.length;\n\n    for (let i = 0; i < longitud; i++) {\n        const nuevoObjeto = {\n            userIDs: diccionarioFiltrado.userIDs[i],\n            fechas: diccionarioFiltrado.fechas[i],\n            stars: diccionarioFiltrado.stars[i],\n            reviews: diccionarioFiltrado.reviews[i],\n        };\n\n        lista.push(nuevoObjeto);\n}\n\n    return {\n        businessId: businessId,\n        reviews: lista,\n    };\n}"
data_to_send['pageFunction']=a+fecha+b

# Convierte el diccionario a formato JSON
json_data = json.dumps(data_to_send)

# Configura las cabeceras
headers = {
    "Content-Type": "application/json"
}

# Envia la solicitud POST con los datos JSON
# response = requests.post(api_url, data=json_data, headers=headers)
response = requests.post(api_url, data=json_data, headers=headers)


# Verifica el estado de la respuesta
if response.status_code == 200:
    print("Solicitud exitosa!")
else:
    print(f"Error en la solicitud: {response.status_code}, {response.text}")

#url = https://us-central1-windy-tiger-410421.cloudfunctions.net/solicitud_datos_nuevos