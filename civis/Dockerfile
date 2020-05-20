FROM continuumio/miniconda:4.7.12

# Having a CLI text editor is nice.
RUN apt-get update && apt-get install -y gdebi-core vim graphviz openssh-client bash-completion


# Install conda dependencies early, as it takes a long time.
RUN conda install -c conda-forge \
  python=3.6 \
  cartopy \
  dask \
  distributed \
  fsspec \
  geopandas>=0.7 \
  intake \
  intake-parquet \
  matplotlib \
  nltk \
  nodejs \
  numpy \
  opencv \
  osmnx \
  pandas>=1 \
  pymc3 \
  python-graphviz \
  r-catools \
  r-devtools \
  r-leaflet \
  r-rgdal \
  r-rpostgres \
  r-rjdbc \
  r-rmysql \
  r-rpostgres \
  r-shinydashboard \
  r-tidytext \
  r-units \
  r-usethis \
  s3fs \
  scikit-learn \
  scipy \
  spacy \
  statsmodels \
  tensorflow \
  xlrd

# R shiny-server needs to be installed after the R package
RUN wget https://download3.rstudio.org/ubuntu-14.04/x86_64/shiny-server-1.5.9.923-amd64.deb -O /tmp/shiny.deb
RUN gdebi -n /tmp/shiny.deb && fix-permissions /var/log/shiny-server /var/lib/shiny-server
RUN rm /tmp/shiny.deb

# Install Oracle Instant Client drivers.
RUN wget https://download.oracle.com/otn_software/linux/instantclient/19600/instantclient-basic-linux.x64-19.6.0.0.0dbru.zip -O instant_client.zip
RUN unzip instant_client.zip && mkdir -p /opt/oracle && mv instantclient_19_6 /opt/oracle/
RUN echo /opt/oracle/instantclient_19_6 > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig
RUN rm instant_client.zip

# Install some stuff from CRAN + Github that is not available on conda-forge.
# env tar for r urbanmapper / github install r packages
ENV TAR="/bin/tar"
RUN R -e "install.packages(c('tidycensus'), dependencies=TRUE, repos = 'http://cran.us.r-project.org')"
RUN R -e "devtools::install_github('UrbanInstitute/urbnmapr')"

# Install some python-only packages using pip
RUN pip install \
  altair \
  arcgis \
  awscli \
  black \
  bqplot \
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
  tableauserverclient \
  usaddress \
  vega-datasets \
  xlrd \
  xlsxwriter

# Environment-related
COPY custom.sh /tmp/custom.sh
RUN cat /tmp/custom.sh >> /etc/bash.bashrc

# Civis-related
RUN pip install civis-jupyter-notebook && \
    civis-jupyter-notebooks-install

ENV TINI_VERSION v0.16.1
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENV DEFAULT_KERNEL <your kernel>  # set to one of python3, python2 or ir
EXPOSE 8888
WORKDIR /root/work
ENTRYPOINT ["/tini", "--"]
CMD ["civis-jupyter-notebooks-start"]
