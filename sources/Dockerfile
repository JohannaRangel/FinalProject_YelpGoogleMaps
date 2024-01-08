FROM python:3.11.6

# Instalar las bibliotecas necesarias
RUN pip install google-cloud-bigquery
RUN pip install pandas
RUN pip install requests
RUN pip install pyarrow

# Copiar tu script al contenedor
COPY pipeline.py /dataflow/
COPY windy-tiger-410421-956bf231305a.json /dataflow/

# Definir el directorio de trabajo
WORKDIR /dataflow/

# Establecer el comando predeterminado para abrir una terminal interactiva
CMD ["/bin/bash"]