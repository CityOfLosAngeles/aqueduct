FROM continuumio/miniconda3:4.8.2

# Having a CLI text editor is nice.
RUN apt-get update && apt-get install -y \
  unzip \
  vim \
  graphviz \
  openssh-client \
  bash-completion \
  gcc


# Install conda dependencies early, as it takes a long time.
RUN conda config --set channel_priority strict
RUN conda install -c conda-forge \
  cartopy \
  dask \
  distributed \
  fsspec \
  geopandas \
  intake \
  intake-parquet \
  matplotlib \
  nltk \
  numpy \
  osmnx \
  pandas \
  python-graphviz \
  s3fs \
  scikit-learn \
  scipy \
  statsmodels \
  xlrd

# Install Oracle Instant Client drivers.
RUN wget https://download.oracle.com/otn_software/linux/instantclient/19600/instantclient-basic-linux.x64-19.6.0.0.0dbru.zip -O instant_client.zip
RUN unzip instant_client.zip && mkdir -p /opt/oracle && mv instantclient_19_6 /opt/oracle/
RUN echo /opt/oracle/instantclient_19_6 > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig
RUN rm instant_client.zip

# Install some python-only packages using pip
RUN pip install \
  altair \
  arcgis \
  awscli \
  black \
  census-data-downloader \
  CensusData \
  contextily \
  cookiecutter \
  cx_Oracle \
  descartes \
  flake8 \
  folium \
  geoalchemy2 \
  geocoder \
  geopy \
  html5lib \
  ibis-framework==1.3 \
  intake-civis \
  intake-dcat \
  intake_geopandas \
  git+https://github.com/intake/intake-sql.git@7709beb#egg=intake_sql \
  ipyleaflet \
  ipywidgets \
  isort \
  jupyterlab \
  kmodes \
  lxml \
  mapboxgl \
  markov_clustering \
  mypy \
  openpyxl \
  papermill \
  partridge \
  polyline \
  psycopg2 \
  requests \
  selenium \
  sodapy \
  sqlalchemy \
  sqlalchemy-redshift \
  tableauserverclient \
  usaddress \
  xlrd \
  xlsxwriter

# Environment-related
COPY custom.sh /tmp/custom.sh
RUN cat /tmp/custom.sh >> /etc/bash.bashrc

# Civis-related
ENV DEFAULT_KERNEL python3
# civis-jupyter-notebook is a bit heavy handed with depenencies.
RUN pip install --no-deps civis-jupyter-notebook civis-jupyter-extensions && \
    civis-jupyter-notebooks-install

ENV TINI_VERSION v0.16.1
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

EXPOSE 8888
WORKDIR /root/work
ENTRYPOINT ["/tini", "--"]
CMD ["civis-jupyter-notebooks-start"]
