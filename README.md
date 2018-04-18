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

7. Edit ``~/airflow/airflow.cfg` file to include location of Sewer repo for DAG bags 

8. Run `airflow webserver` to start the airflow server or `airflow test {dag_name} {task_id} {time}` to test a specfic DAG. 

## Install Issues

1. Installing on a network with a proxy (such as LA City) may require setting proxy settings for these applications. Your proxy server address can be found in your system settings or by asking your network admin. 

1a. Anaconda: (Reference: https://conda.io/docs/user-guide/configuration/use-winxp-with-proxy.html )
Edit the file .condarc (in Windows: \users\{username}\ )
and add these 3 lines (with proxy and port address filled in):
proxy_servers:
    http: http://proxy:port
    https: https://proxy:port
    
1b. Set pip and python proxies by setting the http_proxy and https_proxy system variables to your proxy server. 

1c. Anaconda may call pip to install some modules, but it may not work with the pip proxies you setup. One way around this is to run pip install with proxy options from the command line. After conda failed to pip install flask, run this command line: `pip install --proxy=http://proxy:port flask` and then run the conda line again.
