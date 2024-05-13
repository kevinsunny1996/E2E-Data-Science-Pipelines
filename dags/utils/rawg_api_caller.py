import requests
import pandas as pd
from datetime import datetime

# Logger factory class import
from utils.logger import LoggerFactory

# Custom modules import
from utils.url_generator import generate_full_url

# Common values
base_url = 'https://api.rawg.io/api'
file_save_path = '/usr/local/airflow/include/raw_extracted_data'
endpoint = 'games'

# Initialize loggers for each specific task
info_logger = LoggerFactory.get_logger('INFO')
error_logger = LoggerFactory.get_logger('ERROR')


class RAWGAPIResultFetcher():
  """
    A class that makes GET calls to the RAWG API using the API Key fetched from Airflow Variable.

    Methods:
        get_unique_ids_per_endpoint() -> []:
          Retrieves list of game IDs matching the passed query.
        Returns:
          List containing the Game IDs

        get_game_details_per_id() -> pd.DataFrame:
          Fetches game details for the fetched ID list and flattens them into 5 dataframes [rest 4 has game_id as foreign key] to be later written to GCS as CSV
        Returns:
          N/A as this is a class level comment
  """
  
  def get_unique_ids_per_endpoint(self, api_key: str, page_number: int) -> []:
    """
      Gets unique IDs related to the respective endpoint

      This method sends a GET request to the RAWG API to retrieve
      list of results that are matching the search query.

      Reference endpoint - https://api.rawg.io/docs/

      Args:
          api_key: API Key needed to make calls to RAWG API endpoint.
          endpoint: Endpoint name to be used to get respective data.
          page_number: Page Number for the specific call , to be fetched from Airflow Variable for every run in incremented fashion.
          List of Endpoints: games, creators, tags, developers, platforms, genres etc

      Returns:
          IDs in the form of list using the responses mentioned in the endpoint URL.
    """
    
    info_logger.info(f'Using page number: {page_number} for the following endpoint: {endpoint} call session to get IDs.')

    # List of endpoints to iterate and get IDs from
    # endpoints_list = ['games', 'tags', 'genres', 'developers', 'creators']

    # Empty lists to be later appended while iterating over endpoints
    endpoint_ids_list = []

    # Generate base URL to make a GET call to the respective endpoint
    generate_api_url = generate_full_url(base_url,endpoint,api_key)

    endpoint_params = {
      'page_size': '10000',
      'page': page_number,
      'metacritic': '75'
    }

    endpoint_response = requests.get(generate_api_url, params=endpoint_params)

    # If there is response then only do further processing which involves flattening the data from results list and fetching only IDs
    if endpoint_response.status_code == 200:
      endpoint_response_json = endpoint_response.json()
      flattened_results_df = pd.json_normalize(endpoint_response_json,record_path=['results'])
      endpoint_ids_list = flattened_results_df['id'].tolist()
      info_logger.info(f'Fetched {len(endpoint_ids_list)} unique IDs for {endpoint} endpoint')

    return endpoint_ids_list
  
  def get_game_details_per_id(self, api_key: str, endpoint_ids: list, page_number: int) -> pd.DataFrame:
    """
      Gets games related to the entered search query

      This method sends a GET request to the RAWG API to retrieve details of the game based on the ID used.

      Reference endpoint - https://api.rawg.io/docs/#operation/games_read

      Args:
          self: An instance of the class containing the RAWG API Key.

      Returns:
          Result set in the form of JSON using the responses mentioned in the endpoint URL.

      Raises:
          Exception: If the API call returns a non-200 response code, an exception is raised
          with the status code and error message returned by the API.
    """
    endpoint = 'games'
    payload = {}
    headers = {}

    # Keys to include when flattening /games/id values to create Game Dataframe
    keys_game_ids_df_response = [
      "id", # ID of the said Game
      "slug", # Slug formatted name of the Game eg - Witcher 3 slug version is : witcher-3
      "name_original", # original name of the game , would come handy in visualizing data
      "description_raw", # Description or short summary of the game
      "metacritic", # Metacritic rating of the game 
      "released", # Date when the game was released
      "tba", # Is it an upcoming game or has it been released?
      "updated", # Date when the info regarding the game was last updated
      "rating", # User ratings for the game , its out of 5
      "rating_top", # Rounded off version of rating , for eg 4.01 becomes 4 , 4.99 becomes 5
      "playtime" # Time taken to complete the given game
      ]

    games_df = pd.DataFrame()
    ratings_df = pd.DataFrame()
    platforms_df = pd.DataFrame()
    genre_df  = pd.DataFrame()
    publisher_df = pd.DataFrame()

    # Run a GET call to the games endpoint
    for game_id in endpoint_ids:
      response = requests.request("GET", f"{base_url}/{endpoint}/{game_id}?key={api_key}", headers=headers, data=payload)
      info_logger.info(f'Fetched game related data for {game_id} for {endpoint} endpoint')

      # If the API call is successful and returns response, then only do further processing
      if response.status_code == 200 and response['id'] == game_id:
        # Convert to JSON and flatten the response
        games_json = response.json()

        # Processing steps to create Dataframe containing only game related data
        games_df_raw = pd.json_normalize(games_json, sep='_')
        games_df_filtered = games_df_raw[keys_game_ids_df_response]
        games_df = pd.concat([games_df, games_df_filtered], ignore_index=True)

        # Processing steps to create Dataframe containing only Ratings related Data
        # Use the ID key to use as Foreign Key while loading into DB
        ratings_df_flattened = pd.json_normalize(games_json, sep='_', record_path=['ratings'], meta=['id'], meta_prefix='game_')
        ratings_df = pd.concat([ratings_df, ratings_df_flattened], ignore_index=True)

        # Preprocessing steps to create Dataframe containing platforms data
        platforms_df_flattened = pd.json_normalize(games_json, sep='_', record_path=['platforms'], meta=['id'], meta_prefix='game_')
        platforms_df = pd.concat([platforms_df, platforms_df_flattened], ignore_index=True)
        # Select the columns which contains the name requirements to be later removed as they have unwanted characters.
        # These fields will be later retrieved for upcoming work.
        columns_to_remove = platforms_df.filter(like='requirements_').columns
        info_logger.info(f"Following columns will be removed from platforms_df - {columns_to_remove}")
        platforms_df = platforms_df.drop(columns=columns_to_remove)

        # Preprocessing steps to create Dataframe containing Genre data
        genre_df_flattened = pd.json_normalize(games_json, sep='_', record_path=['genres'], meta=['id'], meta_prefix='game_')
        genre_df = pd.concat([genre_df, genre_df_flattened], ignore_index=True)
        
        # Preprocessing steps to create Dataframe containing Publisher data
        publisher_df_flattened = pd.json_normalize(games_json, sep='_', record_path=['publishers'], meta=['id'], meta_prefix='game_')
        publisher_df = pd.concat([publisher_df, publisher_df_flattened], ignore_index=True)

    # Add load_date column for all dataframes
    games_df['load_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    genre_df['load_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    platforms_df['load_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    publisher_df['load_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    ratings_df['load_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Enforcing datatypes for columns of games dataframe
    games_df['id'] = games_df['id'].astype(int)
    games_df['released'] = pd.to_datetime(games_df['released'], format='%Y-%m-%d')
    games_df['released'] = games_df['released'].astype(str)
    games_df['updated'] = pd.to_datetime(games_df['updated'])
    games_df['updated'] = games_df['updated'].astype(str)
    games_df['rating_top'] = games_df['rating_top'].astype(int)
    games_df['playtime'] = games_df['playtime'].astype(int)
    games_df['metacritic'] = games_df['metacritic'].astype(str)
    games_df['load_date'] = games_df['load_date'].astype(str)

    # Enforcing datatypes for columns of genres dataframe
    genre_df['id'] = genre_df['id'].astype(int)
    genre_df['games_count'] = genre_df['games_count'].astype(int)
    genre_df['game_id'] = genre_df['game_id'].astype(int)
    genre_df['load_date'] = genre_df['load_date'].astype(str)

    # Enforcing datatypes for columns of platforms dataframe
    platforms_df['released_at'] = pd.to_datetime(platforms_df['released_at'], format='%Y-%m-%d')
    platforms_df['released_at'] = platforms_df['released_at'].astype(str)
    platforms_df['platform_id'] = platforms_df['platform_id'].astype(int)
    platforms_df['platform_games_count'] = platforms_df['platform_games_count'].astype(int)
    platforms_df['game_id'] = platforms_df['game_id'].astype(int)
    platforms_df['platform_year_start'] = platforms_df['platform_year_start'].astype(str)
    platforms_df['load_date'] = platforms_df['load_date'].astype(str)

    # Enforcing datatypes for columns of publisher dataframe
    publisher_df['id'] = publisher_df['id'].astype(int)
    publisher_df['games_count'] = publisher_df['games_count'].astype(int)
    publisher_df['game_id'] = publisher_df['game_id'].astype(int)
    publisher_df['load_date'] = publisher_df['load_date'].astype(str)

    # Enforcing datatypes for columns of ratings dataframe
    ratings_df['id'] = ratings_df['id'].astype(int)
    ratings_df['count'] = ratings_df['count'].astype(int)
    ratings_df['percent'] = ratings_df['percent'].astype(float)
    ratings_df['game_id'] = ratings_df['game_id'].astype(int)
    ratings_df['load_date'] = ratings_df['load_date'].astype(str)


    # Log the dimensions of each flattened file for tracking
    info_logger.info(f"Dimension of the data fetched and flattened for the following #{page_number} iteration: Games Table {games_df.shape}, Ratings Table {ratings_df.shape}, Platforms Table {platforms_df.shape}, Genre Table {genre_df.shape}, Publisher Table {publisher_df.shape}")
    info_logger.info(f"Following are the columns of the games table========")
    info_logger.info(f"{games_df.columns}")
    info_logger.info(f"Following are the datatypes of the games table========")
    info_logger.info(f"{games_df.dtypes}")

    info_logger.info(f"Following are the columns of the ratings table========")
    info_logger.info(f"{ratings_df.columns}")

    info_logger.info(f"Following are the columns of the platforms table========")
    info_logger.info(f"{platforms_df.columns}")

    info_logger.info(f"Following are the columns of the genre table========")
    info_logger.info(f"{genre_df.columns}")

    info_logger.info(f"Following are the columns of the publisher table========")
    info_logger.info(f"{publisher_df.columns}")

    return games_df, ratings_df, platforms_df, genre_df, publisher_df