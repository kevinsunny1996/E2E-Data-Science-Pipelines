# Airflow modules import
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
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

# Instance of Schema to be passed for creating empty tables to be later loaded to
table_schemas = {
    'ratings': [
        {'name': 'id','type': 'INTEGER','mode': 'REQUIRED','description': 'Ratings ID corresponding to the rating given to a game'},
        {'name': 'title','type': 'STRING','mode': 'NULLABLE','description': 'Rating Type Corresponding to the respective Rating ID'},
        {'name': 'count','type': 'INTEGER','mode': 'NULLABLE','description': 'Number of games with the given rating , eg- Recommended for 30 games etc.'},
        {'name': 'percent','type': 'FLOAT','mode': 'NULLABLE','description': 'Percentage of games with the given rating'},
        {'name': 'game_id','type': 'INTEGER','mode': 'REQUIRED','description':'Foreign key to map to gaming product table game ID'}
    ],
    'games': [
        {'name': 'id','type': 'INTEGER','mode': 'REQUIRED','description': 'Game ID for the given game'},
        {'name': 'slug','type': 'STRING','mode': 'NULLABLE','description': 'Slug version of the game name , eg- resident-evil-4'},
        {'name': 'name_original','type': 'STRING','mode': 'NULLABLE','description': 'Actual official name of the game'},
        {'name': 'description_raw','type': 'STRING','mode': 'NULLABLE','description': 'Game Description'},
        {'name': 'metacritic','type': 'FLOAT','mode':'REQUIRED','description': 'Metacritic rating of the game'},
        {'name': 'released','type': 'DATE','mode':'REQUIRED','description':'Release date of the game in format YYYY-MM-DD'},
        {'name': 'tba','type': 'BOOLEAN','mode': 'REQUIRED','description': 'Is the game yet to be announced?'},
        {'name': 'updated','type': 'DATETIME','mode': 'REQUIRED','description': 'Time and date when the data was last updated'},
        {'name': 'rating','type': 'FLOAT','mode': 'REQUIRED','description': 'Rating of the game from 1 to 5'},
        {'name': 'rating_top','type': 'NUMERIC','mode': 'REQUIRED','description': 'Max Average Rating given to that game, relates to id of ratings table'},
        {'name': 'playtime','type': 'FLOAT','mode': 'REQUIRED','description': 'Playtime for the game in minutes'}
    ],
    'genres': [
        {'name': 'id','type': 'INTEGER','mode': 'REQUIRED','description': 'Genre ID for the given game'},
        {'name': 'name','type': 'STRING','mode': 'NULLABLE','description': 'Name of the genre , eg- Adventure, Action etc.'},
        {'name': 'slug','type': 'STRING','mode': 'NULLABLE','description': 'Lower case name of the genre , eg- adventure, action etc.'},
        {'name': 'games_count','type': 'INTEGER','mode': 'NULLABLE','description': 'Count of games for that genre'},
        {'name': 'image_background','type': 'STRING','mode': 'REQUIRED','description': 'Image background URL'},
        {'name': 'game_id','type': 'INTEGER','mode': 'REQUIRED','description': 'Game ID , foreign key of Games table'}
    ],
    'platforms': [
        {'name': 'released_at','type': 'DATE','mode': 'NULLABLE','description': 'Release date of the game on the respective platform in format YYYY-MM-DD'},
        {'name': 'platform_id','type': 'INTEGER','mode': 'REQUIRED','description': 'Platform ID for the given platform'},
        {'name': 'platform_name','type': 'STRING','mode': 'REQUIRED','description': 'Platform Name'},
        {'name': 'platform_slug','type': 'STRING','mode': 'REQUIRED','description': 'Lower case platform name'},
        {'name': 'platform_image','type': 'STRING','mode': 'NULLABLE','description': 'Platform Image URL'},
        {'name': 'platform_year_end','type': 'FLOAT','mode': 'NULLABLE','description': 'End Year for the given platform'},
        {'name': 'platform_year_start','type': 'FLOAT','mode': 'NULLABLE','description': 'Start Year for the given platform'},
        {'name': 'platform_games_count','type': 'INTEGER','mode': 'NULLABLE','description': 'Count of games for that platform'},
        {'name': 'platform_image_background','type': 'STRING','mode': 'NULLABLE','description': 'Platform image background URL'},
        {'name': 'game_id','type': 'INTEGER','mode': 'REQUIRED','description': 'Game ID for the given game used as foreign key in games table'}
    ],
    "publishers": [
        {'name': 'id','type': 'INTEGER','mode': 'REQUIRED','description': 'Publisher ID for the given game'},
        {'name': 'name','type': 'STRING','mode': 'REQUIRED','description': 'Name of the publisher'},
        {'name': 'slug','type': 'STRING','mode': 'REQUIRED','description': 'Lower case name of the publisher'},
        {'name': 'games_count','type': 'INTEGER','mode': 'NULLABLE','description': 'Count of games for that publisher'},
        {'name': 'image_background','type': 'STRING','mode': 'NULLABLE','description': 'Image background URL'},
        {'name': 'game_id','type': 'INTEGER','mode': 'REQUIRED','description': 'Game ID for the given game used as foreign key in games table'}
    ]
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
    gcp_project_name = Variable.get('gcp_project_id')
    rawg_bigquery_dataset_name = Variable.get('gcp_bq_dataset')

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
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, ratings_df, 'ratings', rawg_page_number)
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, platforms_df, 'platforms', rawg_page_number)
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, genre_df, 'genres', rawg_page_number)
        get_gcp_connection_and_upload_to_gcs(rawg_landing_gcs_bucket, publisher_df, 'publishers', rawg_page_number)

    # Create Empty Table for Schema defined for the 5 tables created as output
    @task
    def create_rawg_api_placeholder_empty_table(table_name: str, schema: list) -> None:
        create_empty_table_task = BigQueryCreateEmptyTableOperator(
            task_id=f'create_empty_table_for_{table_name}',
            dataset_id=rawg_bigquery_dataset_name,  # Set your BigQuery dataset ID
            table_id=table_name,
            project_id=gcp_project_name,  # Set your BigQuery project ID
            schema_fields=schema,
            gcp_conn_id='gcp',  # Set your GCP connection ID
            if_exists='skip'  # Skip creating the table if it already exists
        )
        create_empty_table_task.execute(context={})
    
    @task_group
    def placeholder_empty_table_task_group():
        for table_name, schema in table_schemas.items():
            create_rawg_api_placeholder_empty_table(table_name, schema)

    @task
    def update_page_number_variable(rawg_page_number: int) -> None:
        # Update page number to fetch from the consecutive one in the next run
        next_page_number = int(rawg_page_number) + 1
        Variable.set("api_page_number", next_page_number)

    # DAG Flow
    game_ids_list = get_rawg_api_game_ids(rawg_api_key, rawg_page_number)
    game_details_extractor = get_game_id_related_data(rawg_api_key, game_ids_list, rawg_page_number)
    placeholder_empty_table_tasks = placeholder_empty_table_task_group()
    page_number_update = update_page_number_variable(rawg_page_number)
    

    game_ids_list >> game_details_extractor >> placeholder_empty_table_tasks >> page_number_update


rawg_api_extractor_dag()

