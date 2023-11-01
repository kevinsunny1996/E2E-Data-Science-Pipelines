# Airflow modules import
from datetime import datetime,timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# Custom modules import
from utils.gcp_utils import CloudUtils


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
@dag(
    dag_id='rawg_data_fetcher_and_load',
    default_args=default_args,
    description='DAG to fetch RAWG API data, convert the JSON to CSV and upload to GCS and then load it in Bigquery',
    schedule=None,
    schedule_interval=None,
    start_date=datetime(2023, 9, 1),
    tags=['rawg_api_elt'],
    catchup=False
)
def data_fetcher_dag():
    @task
    def get_rawg_api_key() -> str:
        return CloudUtils.get_secret('RAWG_API_KEY')

    @task
    def get_rawg_platforms() -> list:
        pass

    @task
    def get_related_spotify_data() -> dict:
        pass

    # store_spotify_output_to_bucket = GCS


