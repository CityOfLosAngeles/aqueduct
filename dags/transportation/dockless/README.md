# ETL Pipeline for Dockless Mobility in Los Angeles

A series of DAGs, created in Apache Airflow, created to process data from dockless mobility providers in Los Angeles

## DocklessDag
#### Tasks
1. Make trips table 
2. Make status change table 
3. use the [mds-provider](http://github.com/cityofsantamonica/mds-provider) library to load a providers data into python, save to s3, then load in to the database under `public.trips` or `public.status_changes`. 

#### Connections
The DAG expects the following connections to be configured in Airflow:

PostgreSQL  
Conn Id: postgres_default  
Host: [host]  
Schema: [db]  
Login: [login]  
Password: [password]  
Port: [port]  

AWS S3  
Conn Id: aws_default  
Extra: {"aws_access_key_id":"[id]","aws_secret_access_key":"[key]"}  

#### Config File 

To upload the .config to S3, run 

`aws s3 cp .config s3://city-of-los-angeles-data-lake/dockless/.config --profile la-city` from the AWS CLI.

## Scoot Stat

A nightly email report on scooter ops. -
### Related Work
* [LA Mobility Data Specification (MDS)](https://github.com/CityOfLosAngeles/mobility-data-specification): A data standard and API specification for mobility as a service providers, such as Dockless Bikeshare, E-Scooters, and Shared Ride providers who work within the public right of way.
