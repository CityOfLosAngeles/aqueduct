# sewer
A shared pipeline for building ETLs and batch jobs that we run at the City of LA for Data Science Projects. Built on Apache Airflow. 

## Contributors 
* Robert Pangalian 
* Hunter Owens

## Setting Up locally 

1. `git clone https://github.com/CityOfLosAngeles/sewer.git`

2. `conda env create sewer # need anaconda python distribution`

3. `source activate sewer`

4.`brew install postgres` If MacOSX, `sudo apt-get install postgresql`  Ubuntu

5. `pip install "apache-airflow[s3, postgres]"`
 
6. Edit airflow.cfg file to include location of Sewer repo for DAG bags 

7. Run `airflow webserver`
