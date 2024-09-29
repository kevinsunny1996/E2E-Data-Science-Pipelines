# Airflow Base Hook to get connection
from airflow.hooks.base import BaseHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# GCS Python Library
from google.cloud import storage, bigquery

# Custom Logging module
from utils.logger import LoggerFactory
import traceback

# Pandas Library import 
import pandas as pd

# Pyarrow Library import
import pyarrow as pa
import pyarrow.parquet as pq

# JSON Library import
import json

# import io 
from io import BytesIO

info_logger = LoggerFactory.get_logger('INFO')
error_logger = LoggerFactory.get_logger('ERROR')

# Function to get Airflow Service Account connection to GCP and write Pandas Dataframe to GCS as CSV File 
def get_gcp_connection_and_upload_to_gcs(bucket_name: str, dataframe_name: pd.DataFrame, blob_file_name: str, api_page_number: int) -> None:
    """
      Common GCP utility to make a connection to GCS using the Service Account and write or save files to GCS bucket 

      Reference implementation for the official client library - https://cloud.google.com/storage/docs/uploading-objects-from-memory

      Args:
          bucket_name: Name of the storage bucket created at GCS end , eg - rawg_api_staging
          dataframe_name: Dataframe processed from rawg_api_caller.py 
          blob_file_name: Placeholder name of the file to be created or used in case if it exists at GCS end, eg - games_2.csv
          api_page_number: Page number being used from Airflow variables to fetch game data from RAWG API in the given run.

      Returns:
          None
    """

    # Airflow service account connection made at Airflow -> Connections end
    airflow_service_account_connection = 'gcp'

    # Get the connection to GCP using the service account
    # gcp_connection_sa = BaseHook.get_connection(conn_id=airflow_service_account_connection)
    gcp_file_upload_hook = GoogleBaseHook(gcp_conn_id=airflow_service_account_connection)
    sa_creds = gcp_file_upload_hook.get_credentials()
    # info_logger.info(f'Retrieved connection: {gcp_connection_sa.conn_id}') 
    
    # Get the credentials from the connection
    # gcp_connection_sa_credentials = gcp_connection_sa.extra_dejson['key_path']
    # info_logger.info(f'Retrieved credentials: {gcp_connection_sa_credentials}') 

    # Initialize GCS client
    # client = storage.Client.from_service_account_json(gcp_connection_sa_credentials)
    client = storage.Client(credentials=sa_creds,project=gcp_file_upload_hook.project_id)

    # Intialize bucket object 
    bucket = client.bucket(bucket_name)

    # Save the pandas dataframe into a PyArrow table
    product_parquet_table = pa.Table.from_pandas(dataframe_name, preserve_index=False)
    # Convert dataframe to CSV string
    # parquet_data = dataframe_name.to_parquet(index=False)

    parquet_data = BytesIO()
    # Write the parquet data to a BytesIO object in memory
    pq.write_table(product_parquet_table, parquet_data)

    # Upload Parquet to GCS 
    try:
        # blob_name is the file name that needs to be created as placeholder at GCS end to write file into
        blob_name = f'{blob_file_name}_{api_page_number}.parquet'
        blob = bucket.blob(blob_name)
        blob.upload_from_string(parquet_data.getvalue(), content_type='application/octet-stream')
        info_logger.info(f'Loaded file {blob_name} to bucket {bucket_name} successfully')
    except Exception as e:
        error_logger.error(f'Received following error while uploading {blob_name}, see full trace : {e}')
        # Print stacktrace for the whole error in the console
        traceback.print_exc()


# Function to get Airflow Service Account connection to GCP and check existence of Game IDs in Bigquery tables before loading
def check_bq_tables_for_extracted_game_ids(extracted_game_ids: list, bq_dataset: str, project_id: str):
    # TODOS
    """
    Using the list received in the extraction call , convert it into comma separated string to be sent into IN clause.
    Use bridge_games_genre or any bridge table as the problem happens for these table during merge incremental update as source has duplicates.
    Fetch the distinct game_id for all those in the extracted list and compare with the list values.
    Log the following details - Comma separated string , game_id present in bridge table for the list, final result for visibility.
    Return the modified list to curb duplicate entries from forming in source tables in the first place.
    """
    bq_conn = 'gcp'

    # Create a BigQuery Hook 
    bq_hook = BigQueryHook(
        gcp_conn_id=bq_conn,
        delegate_to=None,
        use_legacy_sql=False,
        location=None,
        api_resource_configs=None,
        impersonation_chain=None,
    )

    # Stitch the game ID's to a comma separated string
    list_of_ids_to_detect = ','.join([str(game_id) for game_id in extracted_game_ids])
    info_logger.info(f'List of IDs extracted converted to comma separated string format: {list_of_ids_to_detect}')

    # Query to run against the extracted IDs
    detector_query = f"""
        SELECT DISTINCT(game_id) FROM
        `{project_id}.{bq_dataset}.bridge_games_genre` WHERE
        game_id IN ({list_of_ids_to_detect})
    """
    info_logger.info(f'Preparing to run the following query: {detector_query}')

    # Run the query and store the results to be sent downstream
    conn = bq_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(detector_query)

    ids_present_in_bq = [row[0] for row in cursor.fetchall()]
    info_logger.info(f'following are the game ids present in BigQuery table - {ids_present_in_bq}')

    # Run against the extracted list and get the difference
    cleaned_game_ids = [game_id for game_id in extracted_game_ids if game_id not in ids_present_in_bq]
    info_logger.info(f'Returned difference - {cleaned_game_ids}')

    return cleaned_game_ids