# Airflow Base Hook to get connection
from airflow.hooks.base import BaseHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# GCS Python Library
from google.cloud import storage

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