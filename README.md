Overview
========

Data Pipelines for an upcoming End to End Data Science Project...
Currently , EL pipeline is in progress.
For transform stage , we'll be using DBT cloud to work on so that will be tracked under a separate repository.

Flow Diagram
=============

![Airflow DAG Tasks](Airflow_dag_steps)

Salient Features
================

The airflow pipeline uses the following features of Airflow and Astronomer:
- `Hooks`: Uses `GCSHook` to delete files from GCS post load and `GoogleBaseHook` to get service account connection details that have been uploaded to Astro Cloud connections section.
- `Parquet File Creation`: Makes use of PyArrow compatibility with Pandas to allow created dataframe to be saved in an space efficient, schema reliable way ensuring quicker loads and lesser data overflow to other columns.
- `Page Number Based File Naming`: All the created parquet files are saved as `dfname_pagenum.parquet` onto GCS enabling easier tracking via logs and fixes in case of any issues while loading or creating input files.
- `Centralized Logger Factory`: Uses centralized logger so that the python custom modules running the extraction and file save can output similar format log messages while running. Logger is not invoked in airflow DAG code as the heavy lifting is done by these custom python code which are in utils path and other operators have their own logging enabled.
- `Astro Cloud Deployment Hibernation`: The deployment is a DEV deployment with hibernation schedule kept to control costs related to operating managed airflow.

Project Contents
================

This project created using astro cli contains the following parts:

- dags: This folder contains the Python files for your Airflow DAGs.
    - `rawg_api_extractor_dag`: This DAG walks through the EL (Extract And Load with slight transforms of flattening json data and enforcing datatype restrictions on dataframe columns) process of extracting data from RAWG API and loading it into Bigquery.

        - The pipeline has the following sections :
            - #### Extract Section:
                - `get_rawg_api_game_ids`: Fetches a list of Game ID's. Uses the following parameters:
                    - `page_size`: How many results can be shown in a single call.
                    - `page`: Current page number for that API call.
                    - `ordering`: How do you want the search results to be arranged , for this one , we are ordering based on game release date.
                    - `parent_platforms`: Parent platform IDs for which these games were released , can pass IDs in comma separated fashion.
                    - `dates`: Start date and End date range for which you need game data for. In this case its from 1990-01-01 to 2023-12-31.
                
                - `get_game_id_related_data`: Makes a series of calls using the retrieved list of Game IDs to `/games/{id}` endpoint and calls the following custom python callables:
                    - `get_game_details_per_id`: Gets game details per Game ID by making a call to the above mentioned endpoint, append and convert to JSON and flatten to tables using `json_normalize` feature of pandas. Post that the dtypes of each column are updated to match the bigquery column datatypes while loading.
                    - `get_gcp_connection_and_upload_to_gcs`: Writes the pandas dataframes received from the first call to PyArrow Parquet table object in memory using BytesIO. Post that the objects are written to GCS as parquet file blobs using GCS client library.

            - #### Load Section:
                - The Load Section uses `GCSToBigQueryOperator` to load the files onto respective BigQuery tables.

                - The tasks that perform the load are as follows:
                    - `load_rawg_api_ratings_data_to_bq`
                    - `load_rawg_api_games_data_to_bq`
                    - `load_rawg_api_genres_data_to_bq`
                    - `load_rawg_api_platforms_data_to_bq`
                    - `load_rawg_api_publishers_data_to_bq`

                - All of these tasks have the following configurations:
                    - `task_id`: Unique identifier for each load task for respective product table.
                    - `bucket`: Source GCS bucket to pick the file from.
                    - `source_objects`: Source Parquet file name.
                    - `source_format`: Format of the file present in GCS.
                    - `destination_project_dataset_table`: Bigquery table to load the file onto , called using `dataset_name.table_name`
                    - `gcp_conn_id`: Service Account connection given to airflow to perform the EL tasks.
                    - `allow_quoted_newlines`: This allows newline characters within a field to be considered , especially the games table has a description_raw column which has these characters.
                    - `ignore_unknown_values`: Allow unknown values , for example , there are certain studio names with Japanese text so that needs to be considered.
                    - `schema_fields`: Schema of the table based on the output parquet file , also required when a table needs to be created if it doesn't exist which this operator excels in.
                    - `create_disposition`: Create destination table according to the schema and table name if not present.
                    - `autodetect`: Autodetect source file column field datatypes based on the field values , explicitly turned off to avoid override , default value is `True` for this parameter.
                    - `write_disposition`: There are multiple options , for this project we have used `WRITE_APPEND` to allow the upcoming data to get appended to the respective BigQuery Table while loading.
                    - `skip_leading_rows`: Use this to skip certain number of rows in the source file , for this case , we set it to `1` to ignore the headers from getting appended to the table data.

            - #### Miscellaneous Section:
                - These steps / tasks are done to allow the next iteration to use the incremented page number or to cleanup the parquet files present in GCS bucket.
                    - `remove_extracted_api_parquet_files`: Uses `GCSHook` to list the objects present in GCS and iterates over each item and delete them using GCSHook `delete` method.
                    - `update_page_number`: Updates the page number airflow variable by 1 so that next run takes into account the next page number results.


- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running `astro dev start`.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running `docker ps`.

Note: Running `astro dev start` will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

4. Connection testing is [disabled by default](https://docs.astronomer.io/learn/connections#:~:text=You%20can%20enable%20connection%20testing,Enabled%20in%20your%20Airflow%20environment.) , to do so , perform the following steps:
    1. Add the following command - `ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled`  in the Dockerfile.
    2. Restart the setup by typing - `astro dev restart`

5. Post enabling connection testing , create a file in /include/gcp directory as service_account.json , add the download service account key details and add the file name to gitignore to avoid misuse.

6. Now add the following details as connections:
    1. Connection Id - gcp
    2. Connection Type - Google Cloud `Dropdown Selection`
    3. Description - Airflow Service Account connection
    4. For the **Keyfile Path** use the following command to get the path within the pod - `astro dev bash` and type pwd:
        1. Add the following value - /usr/local/airflow/include/gcp/service_account.json
    5. Click on Test and verify success and you're all set.
    
Deploy Your Project to Astronomer
=================================

The DAG code gets deployed to Astronomer cloud through Github Actions pipeline which uses API TOKEN and DEPLOYMENT ID and triggers on demand using `workflow_dispatch` mode.

Challenges Faced
================

- #### Page Number update logic was created but it was getting updated even though the DAG was not run.
    - To avoid such cases , one must place the logic inside a task decorator within the DAG decorator scope and it should solve the issue.

- #### While writing to GCS , the input being passed in the function was the dataframe object so it was unable to find the blob name to be considered and was getting the dataframe data so was failing with Bad Request.
    - For that we created multiple method calls and passed a parameter as the name of the file it will create as a blob object.
    - Refer the following function definition in `gcp_utils.py` which has blob_file_name field in it.
        - def get_gcp_connection_and_upload_to_gcs(bucket_name: str, dataframe_name: pd.DataFrame, blob_file_name: str, api_page_number: int) -> None:

- #### Earlier these files were getting saved as CSVs onto GCS , while loading page number 11 extracts , it failed with the error stating column tba which is a boolean field was getting populated with the release date value, basically data overflow.
    - The cause couldn't be identified as to what was causing the data overflow to happen , however it seems that the games dataframe has description_raw column which has paragraphs of details for each game which might be causing this to happen.
    - Changing the file from CSV to parquet did the trick as it saves the dataframe into schema reliable format suitable for these scenarios.

- #### Parquet file load failed with the following error: google.api_core.exceptions.BadRequest: 400 Error while reading data, error message: Parquet column 'released' has type INT64 which does not match the target cpp_type INT32. reason: invalid
    - To fix this , schema field was kept as STRING and the dataframe column was changed to `str` type.
    - Feel free to refer the following [reddit post](https://www.reddit.com/r/bigquery/comments/1c9mur5/help_needed_in_loading_a_parquet_file_from_gcs_to/) for further details in case you encounter similar issues.
