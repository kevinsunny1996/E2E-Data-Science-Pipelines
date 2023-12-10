import random

from utils.logger import LoggerFactory

# Dictionary with endpoint name and corresponding upper limit
# endpoints_with_upper_page_limits = {
#     'tags': 20,
#     'creators': 25,
#     ''
# }
logger = LoggerFactory.get_logger('INFO')

def get_random_page_number():
    page_number = random.randint(1,20)
    logger.info(f'Page number generated is: {page_number}')
    return page_number

if __name__ == "__main__":
    get_random_page_number()