# Airflow modules import
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
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
        "type": "DATE",
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
        "type": "TIMESTAMP",
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
    }
]

# Schema for publishers table
schema_publishers = [
    {
        "name": "id",
        "mode": "NULLABLE",
        "type": "FLOAT",
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
        "type": "FLOAT",
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
    }
]

# DAG definition to Extract and Load data obtained from RAWG API to Bigquery with current schedule of running every 6 minutes
@dag(
    dag_id='rawg_api_extractor_dag',
    default_args=default_args,
    description='DAG to fetch RAWG API data from games/ endpoint, convert the JSON to CSV and upload to GCS and then load it in Bigquery',
    schedule=None,
    schedule_interval='*/6 * * * *',
    start_date=datetime(2023, 9, 1),
    tags=['rawg_api_elt'],
    catchup=False
)
def rawg_api_extractor_dag():

    # Fetch variable values for API Key and Page Number and Landing bucket URL
    rawg_api_key = Variable.get('rawg_api_key')
    rawg_page_number = int(Variable.get('api_page_number', default_var=1))
    rawg_landing_gcs_bucket = Variable.get('gcs_rawg_api_landing_bucket')
    rawg_api_bq_dataset = Variable.get('gcp_bq_dataset')
    gcp_project_name = Variable.get('gcp_project_id')

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
            Fetches individual entries for Game ID which will be flattened to convert to 5 csv tables and writes the CSV files to remote Cloud Storage.

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
        # get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, ratings_df, 'ratings', rawg_page_number)
        # get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, platforms_df, 'platforms', rawg_page_number)
        # get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, genre_df, 'genres', rawg_page_number)
        # get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, publisher_df, 'publishers', rawg_page_number)

    
    # Load contents from GCS onto BigQuery for that run
    # load_rawg_api_ratings_data_to_bq = GCSToBigQueryOperator(
    #         task_id=f'load_ratings_to_bq',
    #         bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
    #         source_objects=[f'ratings_{rawg_page_number}.csv'],  # Set the name of the CSV file in GCS
    #         source_format='csv',
    #         destination_project_dataset_table=f'{rawg_api_bq_dataset}.ratings',  # Set your BigQuery table name to load the data to.
    #         gcp_conn_id='gcp',  # Set your GCP connection ID.
    #         create_disposition='CREATE_IF_NEEDED',
    #         write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
    #         skip_leading_rows=1 # Skip the header row in the CSV file.
    # )

    load_rawg_api_games_data_to_bq = GCSToBigQueryOperator(
            task_id=f'load_games_to_bq',
            bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
            source_objects=[f'games_{rawg_page_number}.parquet'],  # Set the name of the CSV file in GCS
            source_format='PARQUET',
            allow_quoted_newlines=True,
            ignore_unknown_values=True,
            # max_bad_records=40,
            destination_project_dataset_table=f'{rawg_api_bq_dataset}.games',  # Set your BigQuery table name to load the data to.
            gcp_conn_id='gcp',  # Set your GCP connection ID.
            create_disposition='CREATE_IF_NEEDED',
            schema_fields=schema_games,
            autodetect=False,
            write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
            skip_leading_rows=1 # Skip the header row in the CSV file.
    )

    # load_rawg_api_genres_data_to_bq = GCSToBigQueryOperator(
    #         task_id=f'load_genres_to_bq',
    #         bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
    #         source_objects=[f'genres_{rawg_page_number}.csv'],  # Set the name of the CSV file in GCS
    #         source_format='csv',
    #         destination_project_dataset_table=f'{rawg_api_bq_dataset}.genres',  # Set your BigQuery table name to load the data to.
    #         gcp_conn_id='gcp',  # Set your GCP connection ID.
    #         create_disposition='CREATE_IF_NEEDED',
    #         write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
    #         skip_leading_rows=1 # Skip the header row in the CSV file.
    # )

    # load_rawg_api_platforms_data_to_bq = GCSToBigQueryOperator(
    #         task_id=f'load_platforms_to_bq',
    #         bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
    #         source_objects=[f'platforms_{rawg_page_number}.csv'],  # Set the name of the CSV file in GCS
    #         source_format='csv',
    #         destination_project_dataset_table=f'{rawg_api_bq_dataset}.platforms',  # Set your BigQuery table name to load the data to.
    #         gcp_conn_id='gcp',  # Set your GCP connection ID.
    #         create_disposition='CREATE_IF_NEEDED',
    #         write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
    #         skip_leading_rows=1 # Skip the header row in the CSV file.
    # )

    # load_rawg_api_publishers_data_to_bq = GCSToBigQueryOperator(
    #         task_id=f'load_publishers_to_bq',
    #         bucket=rawg_landing_gcs_bucket,  # Set your GCS bucket name to pick file from.
    #         source_objects=[f'publishers_{rawg_page_number}.csv'],  # Set the name of the CSV file in GCS
    #         source_format='csv',
    #         destination_project_dataset_table=f'{rawg_api_bq_dataset}.publishers',  # Set your BigQuery table name to load the data to.
    #         gcp_conn_id='gcp',  # Set your GCP connection ID.
    #         create_disposition='CREATE_IF_NEEDED',
    #         write_disposition='WRITE_APPEND',  # If the table already exists, BigQuery appends the data to the table.
    #         skip_leading_rows=1, # Skip the header row in the CSV file.
    #         autodetect=False,
    #         schema_fields=schema_publishers,
    #         allow_quoted_newlines=True,
    #         ignore_unknown_values=True,
    #         max_bad_records=40
    # )

    # @task
    # def remove_extracted_api_csv(bucket_name: str) -> None:
    #     rawg_api_gcs_hook = GCSHook(gcp_conn_id='gcp')

    #     # Get the files to delete
    #     api_csv_files = rawg_api_gcs_hook.list(bucket_name)

    #     # Delete the files
    #     for api_file in api_csv_files:
    #         rawg_api_gcs_hook.delete(bucket_name, api_file)

    # @task
    # def update_page_number(rawg_page_number: int) -> int:
    #     # Update page number to fetch from the consecutive one in the next run
    #     next_page_number = int(rawg_page_number) + 1
    #     Variable.set("api_page_number", next_page_number)

    # DAG Flow
    game_ids_list = get_rawg_api_game_ids(rawg_api_key, rawg_page_number)
    game_details_extractor = get_game_id_related_data(rawg_api_key, game_ids_list, rawg_page_number)
    # clear_extracted_csv_files = remove_extracted_api_csv(rawg_landing_gcs_bucket)
    # next_page_number = update_page_number(rawg_page_number)

    game_ids_list >> game_details_extractor >> load_rawg_api_games_data_to_bq
    # >> load_rawg_api_ratings_data_to_bq >> load_rawg_api_games_data_to_bq >> load_rawg_api_genres_data_to_bq >> load_rawg_api_platforms_data_to_bq >> load_rawg_api_publishers_data_to_bq >> clear_extracted_csv_files >> next_page_number


rawg_api_extractor_dag()

