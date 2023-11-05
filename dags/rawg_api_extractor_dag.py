# Airflow modules import
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# Custom modules import




# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
@dag(
    dag_id='rawg_api_extractor_dag',
    default_args=default_args,
    description='DAG to fetch RAWG API data, convert the JSON to CSV and upload to GCS and then load it in Bigquery',
    schedule=None,
    schedule_interval=None,
    start_date=datetime(2023, 9, 1),
    tags=['rawg_api_elt'],
    catchup=False
)
def rawg_api_extractor_dag():

    # Using SecretsManagerHook we make a call the GCP Secrets Manager and get the corresponding API Key
    @task
    def get_rawg_api_key() -> str:
        """
            Fetches API Key from Secrets Manager.

            Args:
                gcp_conn_id: Service Account Connection added to Airflow which has read access to Secrets Manager.
                secret_id: Secret Name with which it has been named at Secrets Manager end.

            Returns:
                API Key as a string value.
        """
        secrets_manager_hook = SecretsManagerHook(gcp_conn_id='gcp')
        api_key = secrets_manager_hook.get_secret(secret_id='RAWG_API_KEY')
        
        return api_key

    # TODO - Write tasks that calls different endpoints of RAWG API using the API Key and upload results to GCS
    @task
    def generate_base_url(api_key: str) -> list:
        base_url = 'https://api.rawg.io/api'
        endpoint = 'games'
        
        return generate_full_url(base_url, endpoint, api_key)

    

    api_key = get_rawg_api_key()
    get_url = generate_base_url(api_key)

rawg_api_extractor_dag()

