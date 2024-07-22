from airflow.hooks.base_hook import BaseHook
import json
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from cosmos import ProfileConfig

from utils.logger import LoggerFactory

info_logger = LoggerFactory.get_logger('INFO')
error_logger = LoggerFactory.get_logger('ERROR')

# Method to create DBT profile based on the retrieved GCP connection and return it as a dictionary
def create_dbt_profile(gcp_connection: str, gcp_project: str, gcp_bq_dataset: str) -> ProfileConfig:
    """
    Create a DBT profile configuration for BigQuery using a Google Cloud Service Account.

    Args:
        gcp_connection (str): The name of the Google Cloud Platform (GCP) connection in Airflow.
        gcp_project (str): The GCP project ID.
        gcp_bq_dataset (str): The BigQuery dataset name.

    Returns:
        ProfileConfig: The DBT profile configuration.

    """
    # Retrieve the connection object
    bigquery_dbt_rawg_api = BaseHook.get_connection(gcp_connection).extra_dejson
    info_logger.info(f'BigQuery DBT Connection Details: {bigquery_dbt_rawg_api}')
    bigquery_service_account = json.loads(bigquery_dbt_rawg_api.get('extra__google_cloud_platform__keyfile_dict'))
    info_logger.info(f'BigQuery Service Account: {bigquery_service_account}')
    bigquery_project = bigquery_dbt_rawg_api.get('project') 
    info_logger.info(f'BigQuery Project: {bigquery_project}')


    # Configure the profile for BigQuery using a Google Cloud Service Account Dictionary
    profile_config = ProfileConfig(
      profile_name = 'rawg_api_transformer',
      target_name = 'prod',
      # Profile mapping
      profile_mapping = GoogleCloudServiceAccountDictProfileMapping(
        conn_id= gcp_connection,
        profile_args={
          'project': gcp_project,
          'dataset': gcp_bq_dataset,
          'threads': 1,
          'keyfile_json': bigquery_service_account,
          'location': 'us-east1'
        }
      )
    )

    return profile_config