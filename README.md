# Developing Civis jobs

Civis provides a platform to schedule jobs to run regularly.
This includes a variety of canned data imports/exports,
Jupyter notebooks, Python, R, and SQL scripts, and so-called "container scripts".
This document focuses on container scripts, as they provide the most flexibility,
and allow us to more easily keep jobs in version control.

## Container scripts

Civis container scripts are:
1. A Docker image.
1. A git repository to clone into the container.
1. A command to run in the container.

Since docker images can do essentially anything, container scripts provide
the most flexible way to interact with the Civis job scheduling platform.

We provide a [Docker image](./Dockerfile) in this repository,
which is published [here](https://hub.docker.com/r/cityofla/ita-data-civis-lab).
This image contains a number of commonly used Python libraries and tools
which are commonly used by this team, and is intended to be a convenient
starting place when developing container scripts.
However, if the image is insufficient for your needs, you can
* Provide your own image, whether based on this one or a completely separate one, or
* Submit a PR adding some functionality to this image.

## Setting up a local environment

This repository includes a local development environment that is intended to mock
the one that will run in production on the Civis Platform.
It includes the following components:
1. A JupyterLab interface, which allows you to run scripts, notebooks, and terminals within the running container.
1. A PostgreSQL container that can be used to mock the Civis PostgreSQL database.
1. A second PostgreSQL container that can be used to mock the Civis Redshift database.
1. TODO: A pgAdmin container to inspect the data in those databases.

It is important to note the limitations of the two mock databases.
The primary way to interact with the Civis databases when not logged into the platform
is via the Civis [Python API](https://civis-python.readthedocs.io/en/stable).
There is no way to locally create other Python interfaces to the databases due to
their virtual private cloud policies. When you are in the platform, however,
you can create Python DBAPI connections or SQLAlchemy engines.
Therefore, when developing locally, there are two options:

1. Develop using the Civis API against the production databases (though possibly using a scratch table/schema to start).
1. Develop using SQLAlchemy engines/DBAPI cursors against the mock databases.

In order to run this locally, you need to have Docker installed on your machine.
Once you have that, you can build the environment with
```bash
docker-compose build
```

You can launch the built environment with
```bash
docker-compose up
```
And navigate to `http://localhost:8888` in a browser.

By default, the container mounts the `aqueduct` repository in the `/app/` directory,
which is the same place to which Civis clones git repositories.
However, the home directory for the container root user is in `/root/work/` (again to match Civis).
If you want to interact with or run scripts that are in this repository in a terminal,
you may want to first execute `cd /app` to get to the right directory.

## Adding credentials

Unless your script doesn't interact with the data warehouse or any other controlled
data sources, you will likely need to attach some credentials.
Civis typically does this by injecting environment variables into a container session.
You will generally need to inject matching variables to run the scripts locally.
Since the variables are usually sensitive, you should add them in a `.env` file
which is not kept in version control.

You can indicate that your script requires certain set of environment variables
by writing a supplemental Docker Compose YAML file that picks up variables from your `.env` file,
and optionally fails if those variables are not set.
For instance, the [bikeshare job](./transportation/bikeshare/trips.yml) includes a supplemental
YAML file which requests login information for the Tableau dashboard which hosts the data.
By using the `:?` syntax, it makes the job fail if those variables are not set:
```yaml
services:
  civis-lab:
    environment:
      - BIKESHARE_USERNAME=${BIKESHARE_USERNAME:?Missing bikeshare username}
      - BIKESHARE_PASSWORD=${BIKESHARE_PASSWORD:?Missing bikeshare password}
```

## Interacting with the Civis data warehouse

If you need to load data to/from the Civis Redshift/PostgreSQL databases,
you can use the Civis Python API. In order to run this, you should
add `CIVIS_API_KEY` to your required environment variables
and set it in your `.env` file.
You should then be able to use the Civis API normally.

If you take this approach, be careful! The database you are interacting
with is the production version. You may want to develop using a scratch schema
before transitioning to using the production schema.

## Interacting with the development data warehouse

You may want or need to use a Python DBAPI cursor or SQLAlchemy engine.
In that case, you can test your code using the mock databases,
and it should still work in the production environment.
For instance, you can conditionally create a SQLAlchemy engine targeting
either the development or production database using the following snippet:

```python
def get_sqlalchemy_engine():
    from urllib.parse import quote_plus
    if os.environ.get("DEV"):
        POSTGRES_URI = os.environ.get("POSTGRES_URI")
    else:
        POSTGRES_URI = (
            f"postgres://"
            f"{quote_plus(os.environ['POSTGRES_CREDENTIAL_USERNAME'])}:"
            f"{quote_plus(os.environ['POSTGRES_CREDENTIAL_PASSWORD'])}@"
            f"{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}"
            f"/{os.environ['POSTGRES_DATABASE']}"
        )
    return sqlalchemy.create_engine(POSTGRES_URI)
```
The `POSTGRES_URI` environment variable is automatically injected into the development environment.
In order to inject the appropriate environment variables into your container script in production,
you should add a parameter to your container script of type "Database (2 dropdowns)".

## Running a job locally

You can run a job locally using docker-compose and merging the main YAML specification
with any supplementary YAML specification you have written for a job.
For instance, the bikeshare job injects a few extra credentials as environment variables,
sets the `command` to execute the script,
and declares that it depends on the PostgreSQL mock database.
You can run it by executing:
```bash
docker-compose -f docker-compose.yml -f transportation/bikeshare/trips.yml run civis-lab
```

Sometimes there are parts of a script you may want to run differently between development and production.
In order to facilitate this, we also inject an environment variable `DEV=1` into the development environment.
You can check this in your scripts to decide how to run things.

## Setting up a container script on the Civis Platform

Once your script is ready, you will want to set it up to run on the Civis Platform
(possibly as a scheduled job).
You can do this with the following steps:
1. Create a new container script using the `Code` dropdown.
1. Select the repository to clone into the container. In this case it will likely be `aqueduct`. Remember that it will be cloned as `/app` in the container.
1. Select the branch/ref to clone (usually, but not always, `master`).
1. Enter the Docker image and tag to run. To use the one provided in this repository, enter `cityofla/ita-data-civis-lab`. Consider going to the dockerhub repository and selecting the [most recent tag](https://hub.docker.com/r/cityofla/ita-data-civis-lab/tags).
1. Choose the resources needed to run the job. You should probably start with the default values, then adjust up and down as necessary.
1. Specify the credentials needed by setting parameters for the script. For instance, you might need to add "Database" and "AWS" credentials. Once the parameters have been set up, you can populate them with specific values.
1. Optionally schedule the job by clicking the "Automate" button.

## Sharing a service

Civis allows you to run a Docker image as a service, which could be a dashboard,
a report, or a deployed app/model. You may want to share a link to that service
with an external stakeholder. In order to do this, you can follow the directions
given [here](./civis-aqueduct-utils/README.md).

## Publishing the image

The [Dockerfile](./Dockerfile) in this repository is intended to be a convenient
image for the City of Los Angeles data team.
It is currently published to [dockerhub](https://hub.docker.com/r/cityofla/ita-data-civis-lab).
In order to publish a new version, you can use the `Makefile`:
```bash
make build
make publish
```
This requires the user to be logged in to the appropriate dockerhub account.
