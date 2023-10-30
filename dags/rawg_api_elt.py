# Airflow modules import
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Custom modules import
from utils.gcp_utils import CloudUtils


# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email': ['<EMAIL>'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
@dag(
    dag_id='rawg_data_fetcher_and_load',
    default_args=default_args,
    description='DAG to fetch RAWG API data and load it in Bigquery',
    schedule=None,
    start_date=days_ago(2),
    tags=['spotify_api_fetcher'],
    catchup=False
)
def data_fetcher_dag():
    @task
    def get_api_key() -> str:
        return CloudUtils.get_secret('RAWG_API_KEY')

    @task
    def get_spotify_ids() -> list:
        pass

    @task
    def get_related_spotify_data() -> dict:
        pass

    # store_spotify_output_to_bucket = GCS


