## ETL Pipeline for Dockless Mobility in Los Angeles

A series of DAGs, created in Apache Airflow, created to process data from dockless mobility providers in Los Angeles

### DocklessDag
#### Tasks
1. `make_provider_tables`: Create postgres destination tables if they do not already exist 
2. `e_[provider]_[feed]`: Call Provider API and dump results into AWS S3 bucket for intermediate storage
3. `clear_[provider]_[table]`: Clear data in the postgres destination tables if data already exists for the provider and time period of the job
4. `tl_[provider]_[feed]`: Open the data from S3 intermediate storage, transform, and load into postgres destination tables 

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

#### Variables
Information needed to connect to Provider APIs are stored within a `provider` variable in Airflow. The value is set to a JSON dictionary with all necessary connection information for that provider API. At a minimum, that dictionary should contain the [baseurl](https://github.com/CityOfLosAngeles/mobility-data-specification/blob/dev/providers.csv) to access the provider API and the token if authentication is required.  
*Example*  
Key: provider  
Val: {"lemon": {"baseurl": "localhost", "token": "None"}}  

### Config File 

To upload the .config to S3, run 

`aws s3 cp .config s3://city-of-los-angeles-data-lake/dockless/.config --profile la-city` from the AWS CLI.


### Related Work
* [LA Mobility Data Specification (MDS)](https://github.com/CityOfLosAngeles/mobility-data-specification): A data standard and API specification for mobility as a service providers, such as Dockless Bikeshare, E-Scooters, and Shared Ride providers who work within the public right of way.
