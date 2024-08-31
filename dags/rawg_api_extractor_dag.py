# os module to get environment vars
import os
from pathlib import Path

# Airflow modules import
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Cosmos an open source Astro library to run DBT jobs
from cosmos import DbtTaskGroup, ProjectConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode, ExecutionMode

# Airflow Exceptions class to skip Airflow tasks when a condition is met
from airflow.exceptions import AirflowSkipException

from airflow.utils.dates import days_ago
from airflow.models import Variable


# Custom modules import
from utils.rawg_api_caller import RAWGAPIResultFetcher
from utils.gcp_utils import get_gcp_connection_and_upload_to_gcs
from utils.dbt_profile import create_dbt_profile


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

DBT_PROJECT_PATH = f"/usr/local/airflow/dags/dbt/games_analyzer_rawg_api"
DBT_EXECUTABLE_PATH = f"/usr/local/airflow/dbt_venv/bin/dbt"

# Schema for games table
schema_games = [
    {
        "name": "id",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
    },
    {
        "name": "slug",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "name_original",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "description_raw",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "released",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "tba",
        "mode": "NULLABLE",
        "type": "BOOLEAN",
        "description": "",
        "fields": []
    },
    {
        "name": "updated",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "rating",
        "mode": "NULLABLE",
        "type": "FLOAT",
        "description": "",
        "fields": []
    },
    {
        "name": "rating_top",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
    },
    {
        "name": "playtime",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
    },
    {
        "name": "metacritic",
        "mode": "",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "load_date",
        "mode": "",
        "type": "STRING",
        "description": "",
        "fields": []
    }
]

# Schema for genre table
schema_genres = [
  {
    "name": "id",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "name",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "slug",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "games_count",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "image_background",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "game_id",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "load_date",
    "mode": "",
    "type": "STRING",
    "description": "",
    "fields": []
  }
]

# Schema for platforms table
schema_platforms = [
  {
    "name": "released_at",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "platform_id",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "platform_name",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "platform_slug",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "platform_image",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "platform_year_end",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "platform_games_count",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "platform_image_background",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "game_id",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "platform_year_start",
    "mode": "",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "load_date",
    "mode": "",
    "type": "STRING",
    "description": "",
    "fields": []
  }
]

# Schema for ratings table
schema_ratings = [
  {
    "name": "id",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "title",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "count",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "percent",
    "mode": "NULLABLE",
    "type": "FLOAT",
    "description": "",
    "fields": []
  },
  {
    "name": "game_id",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "load_date",
    "mode": "",
    "type": "STRING",
    "description": "",
    "fields": []
  }
]

# Schema for publishers table
schema_publishers = [
    {
        "name": "id",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
    },
    {
        "name": "name",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "slug",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "games_count",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
    },
    {
        "name": "image_background",
        "mode": "NULLABLE",
        "type": "STRING",
        "description": "",
        "fields": []
    },
    {
        "name": "game_id",
        "mode": "NULLABLE",
        "type": "INTEGER",
        "description": "",
        "fields": []
    },
    {
        "name": "load_date",
        "mode": "",
        "type": "STRING",
        "description": "",
        "fields": []
    }
]

# DAG definition to Extract and Load data obtained from RAWG API to Bigquery with current schedule of running every 6 minutes
@dag(
    dag_id='rawg_api_extractor_dag',
    default_args=default_args,
    description='DAG to fetch RAWG API data from games/ endpoint, convert the JSON to CSV and upload to GCS and then load it in Bigquery',
    schedule=None,
    # Commenting out interval as load is done in Bigquery
    # schedule_interval='*/3 * * * *',
    # To avoid DAG from triggering when it wakes up from hibernation at 8 UTC
    start_date=datetime(2023, 9, 1),
    # Makes sure only run is active at a point of time to avoid overlapping runs
    # max_active_runs=1,
    tags=['rawg_api_elt'],
    catchup=False,
    max_active_tasks=1
)
def rawg_api_extractor_dag():
  """
  This function defines the DAG (Directed Acyclic Graph) for extracting data from the RAWG API,
  transforming it, and loading it into BigQuery.

  The DAG consists of several task groups:
  - initialize_vars_and_check_hibernation: Checks if the DAG run is close to hibernation time and skips it if so.
  - extract_rawg_api_data: Extracts game IDs from the RAWG API and fetches individual game data for each ID.
  - load_extracted_data_to_bq: Loads the extracted data into BigQuery.
  - post_load_cleanup: Removes the extracted API parquet files and updates the page number for the next run.

  Each task group contains one or more tasks that perform specific operations.

  Note: This code assumes the presence of certain variables in the Airflow Variable store, such as
  'rawg_api_key', 'api_page_number', 'gcs_rawg_api_landing_bucket', 'gcp_bq_dataset', and 'gcp_project_id'.
  """

  rawg_api_key = Variable.get('rawg_api_key')
  rawg_page_number = int(Variable.get('api_page_number', default_var=1))
  rawg_landing_gcs_bucket = Variable.get('gcs_rawg_api_landing_bucket')
  rawg_api_bq_dataset = Variable.get('gcp_bq_dataset')
  gcp_project_name = Variable.get('gcp_project_id')

  # @task_group(group_id='initialize_vars_and_check_hibernation')
  # def initialize_vars_and_check_hibernation():
  #   # Check hibernation is close or not
  #   @task
  #   def check_hibernation(**context):
  #     """
  #     Checks if the DAG run is close to hibernation time and raises an exception to skip the run if so.
  #     """
  #     hibernation_start = datetime.strptime('10:52:00', '%H:%M:%S').time()
  #     hibernation_end = datetime.strptime('22:04:00', '%H:%M:%S').time()
  #     now = datetime.now().time()
  #     if now >= hibernation_start or now <= hibernation_end:
  #       raise AirflowSkipException('Skipping DAG run close to hibernation time')

  #   # Check hibernation task callable
  #   check_hibernation()

  @task_group(group_id='extract_rawg_api_data')
  def extract_rawg_api_data():
    # Task to get Game IDs to fetch data from in subsequent task
    @task
    def get_rawg_api_game_ids(api_key: str, page_number: int) -> []:
      """
      Fetches a list of Game IDs to be later used to make get calls to the RAWG API /games/{id} endpoint.

      Args:
        api_key: The API key needed to make calls to the RAWG API, required for every call per documentation.
        page_number: The page number required to paginate API calls, will increment with every run.

      Returns:
        A list of Game IDs.
      """
      rawg_http_client_games_list = RAWGAPIResultFetcher()
      return rawg_http_client_games_list.get_unique_ids_per_endpoint(api_key, page_number)

    # Task to use the game IDs to iterate upon and create the 5 flattened tables to be uploaded to GCS and then loaded to BigQuery later
    @task
    def get_game_id_related_data(api_key: str, game_ids_list: list, page_number: int) -> None:
      """
      Fetches individual entries for each Game ID, which will be flattened to convert to 5 CSV tables
      and writes the CSV files to remote Cloud Storage.

      Args:
        api_key: The API key needed to make calls to the RAWG API, required for every call per documentation.
        game_ids_list: The list of Game IDs returned from the previous task.
        page_number: The page number required to paginate API calls, will increment with every run.

      Returns:
        None.
      """
      rawg_http_client_game_detail_fetcher = RAWGAPIResultFetcher()
      games_df, ratings_df, platforms_df, genre_df, publisher_df = rawg_http_client_game_detail_fetcher.get_game_details_per_id(api_key, game_ids_list, page_number)

      # Save the files as CSV directly to GCS, post creating GCS bucket variable
      get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, games_df, 'games', rawg_page_number)
      get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, ratings_df, 'ratings', rawg_page_number)
      get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, platforms_df, 'platforms', rawg_page_number)
      get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, genre_df, 'genres', rawg_page_number)
      get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, publisher_df, 'publishers', rawg_page_number)

    # Call the above extraction tasks in the group
    game_ids_list = get_rawg_api_game_ids(rawg_api_key, rawg_page_number)
    get_game_id_related_data(rawg_api_key, game_ids_list, rawg_page_number)

  @task_group(group_id='load_extracted_data_to_bq')
  def load_extracted_data_to_bq():
    # Load contents from GCS onto BigQuery for that run
    load_rawg_api_ratings_data_to_bq = GCSToBigQueryOperator(
      task_id=f'load_ratings_to_bq',
      bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
      source_objects=[f'ratings_{rawg_page_number}.parquet'],  # Set the name of the CSV file in GCS
      source_format='PARQUET',
      destination_project_dataset_table=f'{rawg_api_bq_dataset}.ratings',  # Set your BigQuery table name to load the data to.
      gcp_conn_id='gcp',  # Set your GCP connection ID.
      allow_quoted_newlines=True,
      ignore_unknown_values=True,
      schema_fields=schema_ratings,
      create_disposition='CREATE_IF_NEEDED',
      autodetect=False,
      write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
      skip_leading_rows=1  # Skip the header row in the CSV file.
    )

    load_rawg_api_games_data_to_bq = GCSToBigQueryOperator(
      task_id=f'load_games_to_bq',
      bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
      source_objects=[f'games_{rawg_page_number}.parquet'],  # Set the name of the CSV file in GCS
      source_format='PARQUET',
      allow_quoted_newlines=True,
      ignore_unknown_values=True,
      destination_project_dataset_table=f'{rawg_api_bq_dataset}.games',  # Set your BigQuery table name to load the data to.
      gcp_conn_id='gcp',  # Set your GCP connection ID.
      create_disposition='CREATE_IF_NEEDED',
      schema_fields=schema_games,
      autodetect=False,
      write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
      skip_leading_rows=1 # Skip the header row in the CSV file.
    )

    load_rawg_api_genres_data_to_bq = GCSToBigQueryOperator(
      task_id=f'load_genres_to_bq',
      bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
      source_objects=[f'genres_{rawg_page_number}.parquet'],  # Set the name of the CSV file in GCS
      source_format='PARQUET',
      allow_quoted_newlines=True,
      ignore_unknown_values=True,
      destination_project_dataset_table=f'{rawg_api_bq_dataset}.genres',  # Set your BigQuery table name to load the data to.
      gcp_conn_id='gcp',  # Set your GCP connection ID.
      create_disposition='CREATE_IF_NEEDED',
      write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
      schema_fields=schema_genres,
      autodetect=False,
      skip_leading_rows=1 # Skip the header row in the CSV file.
    )

    load_rawg_api_platforms_data_to_bq = GCSToBigQueryOperator(
      task_id=f'load_platforms_to_bq',
      bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
      source_objects=[f'platforms_{rawg_page_number}.parquet'],  # Set the name of the CSV file in GCS
      source_format='PARQUET',
      allow_quoted_newlines=True,
      ignore_unknown_values=True,
      destination_project_dataset_table=f'{rawg_api_bq_dataset}.platforms',  # Set your BigQuery table name to load the data to.
      gcp_conn_id='gcp',  # Set your GCP connection ID.
      create_disposition='CREATE_IF_NEEDED',
      schema_fields=schema_platforms,
      autodetect=False,
      write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
      skip_leading_rows=1 # Skip the header row in the CSV file.
    )

    load_rawg_api_publishers_data_to_bq = GCSToBigQueryOperator(
      task_id=f'load_publishers_to_bq',
      bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
      source_objects=[f'publishers_{rawg_page_number}.parquet'],  # Set the name of the CSV file in GCS
      source_format='PARQUET',
      destination_project_dataset_table=f'{rawg_api_bq_dataset}.publishers',  # Set your BigQuery table name to load the data to.
      gcp_conn_id='gcp',  # Set your GCP connection ID.
      create_disposition='CREATE_IF_NEEDED',
      write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
      skip_leading_rows=1, # Skip the header row in the CSV file.
      autodetect=False,
      schema_fields=schema_publishers,
      allow_quoted_newlines=True,
      ignore_unknown_values=True,
      max_bad_records=40
    )

    # DAG Flow between load steps
    load_rawg_api_ratings_data_to_bq >> load_rawg_api_games_data_to_bq >> load_rawg_api_genres_data_to_bq >> load_rawg_api_platforms_data_to_bq >> load_rawg_api_publishers_data_to_bq

  @task_group(group_id='post_load_cleanup')
  def post_load_cleanup():
    @task
    def remove_extracted_api_parquet_files(bucket_name: str) -> None:
      """
      Removes the extracted API parquet files from the specified GCS bucket.

      Args:
        bucket_name: The name of the GCS bucket.

      Returns:
        None.
      """
      rawg_api_gcs_hook = GCSHook(gcp_conn_id='gcp')

      # Get the files to delete
      api_parquet_files = rawg_api_gcs_hook.list(bucket_name)

      # Delete the files
      for api_file in api_parquet_files:
        rawg_api_gcs_hook.delete(bucket_name, api_file)

    @task
    def update_page_number(rawg_page_number: int) -> int:
      """
      Updates the page number to fetch from the consecutive one in the next run.

      Args:
        rawg_page_number: The current page number.

      Returns:
        The next page number.
      """
      next_page_number = int(rawg_page_number) + 1
      Variable.set("api_page_number", next_page_number)

    remove_extracted_api_parquet_files(rawg_landing_gcs_bucket) >> update_page_number(rawg_page_number)

  # Define the DAG structure
  # initialize_vars_and_check_hibernation()
  # extract_rawg_api_data()
  # load_extracted_data_to_bq()
  # post_load_cleanup()


  transform_loaded_rawg_data = DbtTaskGroup(
    group_id='cached_venv_dbt_tasks',
    # Profile configuration
    profile_config = create_dbt_profile('gcp_bq_gcs', gcp_project_name, rawg_api_bq_dataset),
    # Project Config 
    project_config = ProjectConfig(
      dbt_project_path=DBT_PROJECT_PATH,
      models_relative_path="models",
      dbt_vars={
        "gcp_project_name": gcp_project_name, 
        "rawg_api_bq_dataset": rawg_api_bq_dataset
      }
    ),
    # Execution Config
    execution_config = ExecutionConfig(
      execution_mode=ExecutionMode.VIRTUALENV,
      virtualenv_dir=Path(DBT_EXECUTABLE_PATH).parent.parent,
      dbt_executable_path=DBT_EXECUTABLE_PATH
    ),
    render_config = RenderConfig(
      load_method=LoadMode.DBT_LS
    ),
    operator_args = {
      "install_deps": True
    }
  )

  # DAG Flow
  # initialize_vars_and_check_hibernation() >> 
  extract_rawg_api_data() >> load_extracted_data_to_bq() >> post_load_cleanup() >> transform_loaded_rawg_data 

rawg_api_extractor_dag()