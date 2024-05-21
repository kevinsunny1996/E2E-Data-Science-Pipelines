from utils.logger import LoggerFactory

logger = LoggerFactory.get_logger('INFO')

def generate_full_url(base_url: str, endpoint_name: str, api_key: str) -> str:
    """
    Creates a full URL from the base URL and the endpoint name.

    Args:
        base_url (str): The base URL of the API.
        endpoint_name (str): The name of the endpoint.
        api_key (str): The API key for the API being used.

    Returns:
        str: A full URL as a string.
    """
    logger.info(f'{base_url}/{endpoint_name}?key={api_key}')
    return f'{base_url}/{endpoint_name}?key={api_key}'


if __name__ ==  "__main__":
    generate_full_url() 