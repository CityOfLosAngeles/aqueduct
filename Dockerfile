FROM continuumio/miniconda3:4.8.2

# Install inkscape and xelatex for notebook conversion to PDF,
# graphviz for ibis visualizations.
RUN apt-get update && apt-get install -y \
  bash-completion \
  gcc \
  graphviz \
  gnupg2 \
  inkscape \
  openssh-client \
  pandoc \
  texlive-fonts-recommended \
  texlive-generic-recommended \
  texlive-xetex \
  unixodbc-dev \
  unzip \
  vim


# Some dependencies necessary to get rendering working
# for headless chrome/chromium.
RUN apt-get update && apt-get install -y gconf-service libasound2 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 libexpat1 libfontconfig1 libgcc1 libgconf-2-4 libgdk-pixbuf2.0-0 libglib2.0-0 libgtk-3-0 libnspr4 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 ca-certificates fonts-liberation libappindicator1 libnss3 lsb-release xdg-utils wget


# Install conda dependencies early, as it takes a long time.
RUN conda config --set channel_priority strict
RUN conda install -c conda-forge \
  cartopy \
  dask \
  distributed \
  fsspec \
  geopandas>=0.8 \
  intake \
  intake-parquet \
  matplotlib \
  nltk \
  nodejs \
  numpy \
  osmnx \
  pandas \
  pyodbc \
  python-graphviz \
  s3fs==0.4.2 \
  scikit-learn \
  scipy \
  statsmodels \
  xlrd

# Install Oracle Instant Client drivers.
RUN wget https://download.oracle.com/otn_software/linux/instantclient/19600/instantclient-basic-linux.x64-19.6.0.0.0dbru.zip -O instant_client.zip
RUN unzip instant_client.zip && mkdir -p /opt/oracle && mv instantclient_19_6 /opt/oracle/
RUN echo /opt/oracle/instantclient_19_6 > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig
RUN rm instant_client.zip

# Install SQL Server ODBC Driver
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Dependencies for altair_saver, conda packages are currently broken.
RUN npm install -g --unsafe vega vega-cli vega-lite canvas

# Install puppeteer-related notebook conversion.
RUN pip install notebook-as-pdf && pyppeteer-install

# Install some python-only packages using pip
RUN pip install \
  altair \
  altair_saver \
  arcgis \
  awscli \
  beautifulsoup4 \
  black \
  boto3 \
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
  intake-civis[ibis,geospatial] \
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
  sodapy \
  sqlalchemy \
  sqlalchemy-redshift \
  tableauserverclient \
  usaddress \
  voila \
  xlrd \
  xlsxwriter

RUN jupyter labextension install --no-build @jupyter-voila/jupyterlab-preview
RUN jupyter labextension install --no-build @jupyter-widgets/jupyterlab-manager
RUN jupyter labextension install --no-build @jupyterlab/geojson-extension
RUN jupyter lab build --dev-build=False --debug && jupyter lab clean

# Civis-related
ENV DEFAULT_KERNEL python3
# civis-jupyter-notebook is heavy handed with dependencies.
# If we want a different version of pandas, we have to reinstall it afterwards.
RUN pip install civis-jupyter-notebook && \
     civis-jupyter-notebooks-install && \
     pip install -U pandas


ENV TINI_VERSION v0.16.1
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
RUN echo `which jupyter-lab`

EXPOSE 8888
WORKDIR /root/work
ENTRYPOINT ["/tini", "--"]

# Install the local utils package
COPY civis-aqueduct-utils /tmp/civis-aqueduct-utils
RUN pip install /tmp/civis-aqueduct-utils && \
      jupyter nbextension install --sys-prefix --py civis_aqueduct_utils && \
      jupyter nbextension enable --sys-prefix --py civis_aqueduct_utils && \
      rm -r /tmp/civis-aqueduct-utils

# Environment-related
COPY custom.sh /tmp/custom.sh
RUN cat /tmp/custom.sh >> /root/.bashrc

# Copy the aws-configure script
COPY aws-configure /usr/local/bin/
RUN chmod +x /usr/local/bin/aws-configure

# Copy the start script
COPY start.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start.sh

CMD ["start.sh"]
