# Data Engineering Challenge - Fire Incidents
Repository for fire incidents - data engineering challenge

## Sections

## Problem Statement
A customer has the need to analyze a dataset of fire incidents in the city of San Francisco. This challenge consists in making the data available in a Data Warehouse and create a model to run dynamic queries efficiently.

### Requirements
* The copy of the dataset in the data warehouse should reflect exactly the current state of the data at the source.
* The dataset is updated daily at the source
* They business intelligence team needs to run queries that aggreagate these incidents along the following dimensions: time period, district, battalion.

### Deliverable
* Include at least one report that shows how to use your model.
* Write a brief description of the provided solution, including your assumptions, thought process, and usage instructions.
* Use any technology you see fit to solve the problem

## Proposed Architecture Overview
Here is an overview about the proposed architecture:
![Proposed architecture](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/proposed-architecture.jpg)

* _Orchestration_: This layer is responsible for job scheduling and can be achieved using Apache Airflow, Glue or any similar tool - for this exercise I'm using Airflow.
* _Processing_: The data provided by the API is not huge, but considering a scenario where we need to scale the processing, distributed processing becomes very relevant. That's why I thought about using Spark (via PySpark) as the processing layer.
* _Storage_: A centralized storage layer that is scalable and allows storing data with different formats is important to join information from multiple perspectives - tools like S3, Google Cloud Storage or Azure Blob Storage can be used as a storage layer in this case.
* _Data Warehousing / Data Lake_: We need a way to query the data stored in the storage layer, and Hive / Presto can be used for this matter. I also like Amazon Redshift, since it allows querying structured data and unstructured data (through Redshift Spectrum), increasing the posibilities of extracting the best information from collected data.
* _Visualization_: There are lots of tools to help visualizing the data, I had contact with Tableau, PowerBI, Grafana among others - all great tools.

## Solution Overview
The solution I developed in this repository aims to implement and be a proof of concept about some of the ideas proposed above.
The environment is composed by the following tools:
* Postgres database: Used to manage Airflow
* Airflow (webserver and scheduler): Used as orchestration layer
    * We are running PySpark scripts within the Airflow instance due to my laptop limitations, but ideally we should have a separate container for Spark.
* MinIO: Used as storage layer, to simulate S3
* PowerBI for visualization

Note: I was not able to create a Hive / Presto instance, as described in [Challenges during development](#challenges-during-development)

### Code Structure
* Environment related files:
    * [docker-compose.yaml](docker-compose.yaml): Main docker compose file, containing the setup for this environment.
    * [Dockerfile_Airflow](Dockerfile_Airflow): Dockerfile to build images for Airflow.
    * [.env.example](.env.example): Configuration files for environment. See the section [How to create the environment](#how-to-create-the-environment) for more information about how to use this file.
    * [./airflow/scripts/entrypoint.sh](airflow/scripts/entrypoint.sh): Bash script that runs on the Airflow container after it gets created, basically creates an admin user and starts the service.
* Data engineering related files:
    * [./airflow/dags/dag_load_fire_incidents.py](./airflow/dags/dag_load_fire_incidents.py): Airflow DAG responsible for orchestrating the ETL job to extract data from the Fire Incidents API.
    * [./airflow/dags/pyspark_scripts/get_fire_incidents_api_data.py](./airflow/dags/pyspark_scripts/get_fire_incidents_api_data.py): PySpark job that import data from the API and save into MinIO formatted as parquet partitioned files.
* Visualization related files:
    * [./powerbi/powerbi_script.py](./powerbi/powerbi_script.py): Script used to get information from the parquet files in MinIO and load into PowerBI for reporting.
    * [./powerbi/dashboard.pbix](./powerbi/dashboard.pbix): PowerBI dashboard with the data connection and super simple visualizations.

### How to run the code
#### Requirements
* [https://docs.docker.com/compose/install/](Docker compose)
* [https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/](Python3 virtual environment (venv))
* Java (I'm using OpenJDK 11.0.21)
* PowerBI
Note: I used WSL to create the environment in a Ubuntu distro image and connected PowerBI using my Windows machine, so Python must be installed in both environments.

#### How to create the environment
_Ubuntu environment:_
1. Rename the file [.env.example](.env.example) to `.env` and change the values accordingly
2. Create a new Python3 virtual environment by running `python3 venv .venv`. After that, activate the environment (`source .venv/bin/activate`) and install the required libraries from [requirements.txt](requirements.txt), running `pip install -r requirements.txt`.
3. Set execution permissions to Airflow's [entrypoint file](./airflow/scripts/entrypoint.sh), by running `sudo chmod +x airflow/scripts/entrypoint.sh`
4. Run the docker compose file to create the environment: `docker compose up -d` (or `docker-compose up -d` depending on your docker compose version)
_Windows environment:_
1. Install the following packages in your python environment `pip install pandas matplotlib pyarrow minio`

#### How to execute the Airflow DAG
##### Daily DAG
1. Open your Airflow instance by accessing [http://localhost:8080](http://localhost:8080) in your browser. Use the credentials user: `admin` and password: `admin` to access the home page (you can change this user if you want, just edit the file [./airflow/scripts/entrypoint.sh](airflow/scripts/entrypoint.sh)).
2. Start the DAG [./airflow/dags/dag_load_fire_incidents.py](airflow/dags/dag_load_fire_incidents.py) by accessing the [http://localhost:8080/dags/dag_load_fire_incidents/grid](DAG detail link) and then clicking the toggle button on the top left.

![Activate DAG](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/airflow-activate-dag.jpg)

3. Wait until all steps are green (except the pink one that is skipped because we are following the daily processing path).

![Airflow all green](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/airflow-all-green.jpg)

4. Access your MinIO instance using the link [http://localhost:9091](http://localhost:9091), providing your credentials (that should be set in your `.env` file). Access your [http://localhost:9001/browser/de-challenge](bucket details) and you should see a folder `fire_incidents`. Inside this folder you should see another folder with the current day and a `_SUCCESS` file (there's no parquet file because the API doesn't provide daily information).

![Minio folder](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/minio-folder.jpg)

##### Processing one specific day
There's one easy way to backfill previous specific days by using `airflow tasks test` command.
In your Ubuntu terminal, type:
`docker exec -it airflow-webserver airflow tasks test dag_load_fire_incidents save_api_daily_results 'YYYY-MM-DD'`, where YYYY-MM-DD should be the date you want to backfill.
After running this command, you should see a `_SUCCESS` file and your parquet files created in your MinIO instance.

![Minio backfilled](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/minio-backfilled.jpg)

##### Processing full history (warning! it takes a long time)
You can process the whole historical information (currently with ~650k entries) by triggering the DAG with parameters.
1. Access the [http://localhost:8080/dags/dag_load_fire_incidents/grid](dag_load_fire_incidents DAG details)
2. On the play bottom on the top left, select "Trigger DAG w/ config"
3. Insert the JSON `{"is_historical_processing": true}` in the "Configuration JSON (Optional, must be a dict object)" and click the button Trigger in the bottom left.
4. Wait until all the steps are green to have all the information loaded.

Or you can run the command below in your Ubuntu terminal:
`docker exec -it airflow-webserver airflow tasks test dag_load_fire_incidents save_api_historical_results 'YYYY-MM-DD'`, where YYYY-MM-DD should be any date in the past.

#### Visualization
For visualization, I have loaded 2 months of data (November/2023 and December/2023) just to test connection and loaded the data in PowerBI using a Python script.
The script [./powerbi/powerbi_script.py](./powerbi/powerbi_script.py) loads information from all parquet files stored in MinIO and aggregates on a pandas dataframe to be used in PowerBI.
In your PowerBI desktop:
* Make sure you have Python installed and configured in your PowerBI, as well as all required libraries installed (check [how to create environment](#how-to-create-the-environment) under Windows section for more information)
* Ensure you have your docker in WSL running and some files loaded into MinIO
* Check the credentials in [./powerbi/powerbi_script.py](./powerbi/powerbi_script.py), they should match with the ones used in your docker compose file 

1. Create a new connection using Python script as a source
![PowerBI get data 1](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/powerbi-get-data1.jpg)
2. Copy the [./powerbi/powerbi_script.py](./powerbi/powerbi_script.py) script, paste into the popup and click OK
![PowerBI get data 2](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/powerbi-get-data2.jpg)
3. Check the `combined_df` checkbox and hit Load
![PowerBI get data 3](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/powerbi-get-data3.jpg)
4. Use the loaded data from MinIO to create your dashboard
![PowerBI get data 4](https://github.com/renaros/de-challenge-fire-incidents/blob/main/readme_images/powerbi-get-data4.jpg)

## Challenges during development
* I wasn't able to have a separate environment for Spark due to limitations with my computer :/
* The API documentation has some columns that doesn't exist when we call the API, so I got some hard time defining the pyspark dataframe schema.
* It took me some time to make PowerBI to work, I tried to create a Hive instance and a Presto instance and failed (due to laptop limitations). Then I tried to run a PySpark script that worked fine on my WSL Ubuntu but failed on my Windows machine. Finally I could make the pandas script work and connected PowerBI to MinIO \0/