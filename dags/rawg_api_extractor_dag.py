# Airflow modules import
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Custom modules import
from utils.rawg_api_caller import RAWGAPIResultFetcher
from utils.gcp_utils import get_gcp_connection_and_upload_to_gcs



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
    description='DAG to fetch RAWG API data from games/ endpoint, convert the JSON to CSV and upload to GCS and then load it in Bigquery',
    schedule=None,
    schedule_interval=None,
    start_date=datetime(2023, 9, 1),
    tags=['rawg_api_elt'],
    catchup=False
)
def rawg_api_extractor_dag():

    # Fetch variable values for API Key and Page Number and Landing bucket URL
    rawg_api_key = Variable.get('rawg_api_key')
    rawg_page_number = int(Variable.get('api_page_number', default_var=1))
    rawg_landing_gcs_bucket = Variable.get('gcs_rawg_api_landing_bucket')

    # Task to get Game IDs to fetch data from in subsequent task
    @task
    def get_rawg_api_game_ids(api_key: str, page_number: int) -> []:
        """
            Fetches List of Game IDs to be later used to make get call to /games/{id} endpoint.

            Args:
                api_key: Needed to make calls to RAWG API , required for every call per documentation.
                page_number: Required to paginate API calls , will increment with every run 

            Returns:
                Game IDs as a list value
        """
        rawg_http_client_games_list = RAWGAPIResultFetcher()     
        return rawg_http_client_games_list.get_unique_ids_per_endpoint(api_key, page_number)

    # Task to use the game ids to iterate upon and create the 5 flattened tables to be uploaded to GCS and then loaded to Bigquery later
    @task
    def get_game_id_related_data(api_key: str, game_ids_list: list, page_number: int) -> None:
        """
            Fetches individual entries for Game ID which will be flattened to convert to 5 csv tables and save to local file path.

            Args:
                api_key: Needed to make calls to RAWG API , required for every call per documentation.
                game_ids_list: List of Game IDs returned from previous task
                page_number: Required to paginate API calls , will increment with every run 

            Returns:
                None
        """
        rawg_http_client_game_detail_fetcher = RAWGAPIResultFetcher()
        games_df, ratings_df, platforms_df, genre_df, publisher_df = rawg_http_client_game_detail_fetcher.get_game_details_per_id(api_key, game_ids_list, page_number)

        # Save the files as CSV directly to GCS , post creating GCS bucket variable
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, games_df, 'games', rawg_page_number)
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, ratings_df, 'ratings', rawg_page_number)
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, platforms_df, 'platforms', rawg_page_number)
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, genre_df, 'genres', rawg_page_number)
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, publisher_df, 'publishers', rawg_page_number)

        # Update page number to fetch from the consecutive one in the next run
        next_page_number = int(rawg_page_number) + 1
        Variable.set("api_page_number", next_page_number)


    game_ids_list = get_rawg_api_game_ids(rawg_api_key, rawg_page_number)
    game_details_extractor = get_game_id_related_data(rawg_api_key, game_ids_list, rawg_page_number)


rawg_api_extractor_dag()

