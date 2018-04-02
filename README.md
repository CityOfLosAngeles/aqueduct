# sewer
A shared pipeline for building ETLs and batch jobs that we run at the City of LA for Data Science Projects. Built on Apache Airflow. 

## Contributors 
* Robert Pangalian 
* Hunter Owens

## Setting Up locally 

1. `git clone https://github.com/CityOfLosAngeles/sewer.git`

2. `conda env create sewer # need anaconda python distribution`

3. `source activate sewer`

If you don't have postgres installed
4.`brew install postgres` If MacOSX, `sudo apt-get install postgresql`  Ubuntu

5. Create conda env `conda env create --name sewer --file environment.yml` or update conda env `conda env update --name sewer --file environment.yml`

6. `airflow initdb`

7. Edit airflow.cfg file to include location of Sewer repo for DAG bags 

8. Run `airflow webserver` to start the airflow server or `airflow test {dag tame} {task name} {time}` to test a DAG. 
