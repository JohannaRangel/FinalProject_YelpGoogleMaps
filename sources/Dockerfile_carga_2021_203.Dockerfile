FROM python:3.11.6

# Instalar las bibliotecas necesarias
RUN pip install google-cloud-bigquery
RUN pip install pandas
RUN pip install geopy
RUN pip install google-cloud-bigquery
RUN pip install google-cloud-storage
RUN pip install google-auth
RUN pip install pyarrow
RUN pip install db-dtypes

# Copiar tu script al contenedor
COPY  YGC_pipline_reviews_2021_2023.py /app/
COPY service_account.json /app/
COPY businessId_gmapID.csv /app/

# Definir el directorio de trabajo
WORKDIR /app/

# Establecer el comando predeterminado para abrir una terminal interactiva
CMD ["/bin/bash"]