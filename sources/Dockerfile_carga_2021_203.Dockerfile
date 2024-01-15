FROM python:3.11.6

# Instalar las bibliotecas necesarias
RUN pip install google-cloud-bigquery
RUN pip install pandas
RUN pip install geopy
RUN pip install google-cloud-bigquery
RUN pip install google-cloud-storage
RUN pip install google-auth


# Copiar tu script al contenedor
COPY pipeline_apache_beam.py /dataflow/
COPY windy-tiger-410421-956bf231305a.json /dataflow/

# Definir el directorio de trabajo
WORKDIR /dataflow/

# Establecer el comando predeterminado para abrir una terminal interactiva
CMD ["/bin/bash"]