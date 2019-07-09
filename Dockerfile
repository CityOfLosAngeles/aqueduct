FROM python:3.7-slim

ENV PYTHONUNBUFFERED=1 \
    AWS_REGION=us-west-2 \
    # AWS_ACCESS_KEY_ID= \
    # AWS_SECRET_ACCESS_KEY= \
    PORT=8000 \
    DEPLOY_ENVIRONMENT=dev \
    DEVELOPMENT=1 \
    DEPLOY_TAG=master 

# Airflow configuration can be set here using the following format:
# $AIRFLOW__{SECTION}__{KEY}
# See also: https://airflow.apache.org/configuration.html

ENV AIRFLOW_HOME=/app \
    AIRFLOW_AUTHENTICATE=False \
    AIRFLOW_AUTH_BACKEND=airflow.contrib.auth.backends.password_auth \
    AIRFLOW_BROKER_URL=redis://redis:6379/0 \
    AIRFLOW_RESULT_URL=redis://redis:6379/0 \
    AIRFLOW_FLOWER_PORT="5555" \
    AIRFLOW_DATABASE_URL=postgres://postgres@db/postgres \
    AIRFLOW_FERNET_KEY="0000000000000000000000000000000000000000000=" \
    AIRFLOW_SECRET_KEY="0000000000000000000000000000000000000000000=" \
    # AIRFLOW_SMTP_HOST= \
    # AIRFLOW_SMTP_USER= \
    # AIRFLOW_SMTP_PASSWORD= \
    AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False \
    SLUGIFY_USES_TEXT_UNIDECODE=yes \
    URL=http://localhost

EXPOSE $PORT


# add a non-privileged user for installing and running the application
RUN mkdir -p /app && \
    chown 10001:10001 /app && \
    groupadd --gid 10001 app && \
    useradd --no-create-home --uid 10001 --gid 10001 --home-dir /app app

# python-slim base image has missing directories required for psql install
RUN mkdir -p /usr/share/man/man1
RUN mkdir -p /usr/share/man/man7

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        apt-transport-https build-essential curl git libpq-dev \
        postgresql-client gettext sqlite3 libffi-dev libsasl2-dev && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install Python dependencies
COPY requirements.txt /tmp/
# Switch to /tmp to install dependencies outside home dir
WORKDIR /tmp

RUN pip install --no-cache-dir -r requirements.txt

# Switch back to home directory
WORKDIR /app

COPY . /app

RUN chown -R 10001:10001 /app

USER 10001

# Using /bin/bash as the entrypoint works around some volume mount issues on Windows
# where volume-mounted files do not have execute bits set.
# https://github.com/docker/compose/issues/2301#issuecomment-154450785 has additional background.
ENTRYPOINT [ "/bin/bash", "/app/bin/run.sh"]

CMD ["web"]