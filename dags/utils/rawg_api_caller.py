import requests

# Logger factory class import
from logger import LoggerFactory

# Custom modules import
from gcp_utils import CloudUtils
from url_generator import generate_full_url

base_url = 'https://api.rawg.io/api'

class RAWGAPIResultFetcher():
  """
    A class that makes GET calls to the RAWG API using the API Key fetched from secrets manager.

    Attributes:
        api_key: A string representing the API Key for your RAWG Developer Account.

    Methods:
        get_games() -> {}:
          Retrieves list of games matching the passed query.
        Returns:
          JSON Response for the endpoint - https://api.rawg.io/docs/#operation/games_list
  """
  
  def get_games(api_key: str) -> {}:
    """
      Gets games related to the entered search query

      This method sends a GET request to the RAWG API to retrieve
      list of games that are matching the search query.

      Reference endpoint - https://api.rawg.io/docs/#operation/games_list

      Args:
          self: An instance of the class containing the RAWG API Key.

      Returns:
          Result set in the form of JSON using the responses mentioned in the endpoint URL.

      Raises:
          Exception: If the API call returns a non-200 response code, an exception is raised
          with the status code and error message returned by the API.
    """
    endpoint = 'games'
    game_request_url = generate_full_url(base_url, endpoint, api_key)
    payload = {}
    headers = {}

    # Run a GET call to the games endpoint
    response = requests.request("GET", game_request_url, headers=headers, data=payload).json()

    if response.status_code != 200:
      raise Exception('Error:', response.status_code, response.text)
    
    LoggerFactory.get_logger('./logs/info.log','INFO').info(f'Fetched {len(response["results"])} games for the current session')