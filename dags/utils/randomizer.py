import random

# Dictionary with endpoint name and corresponding upper limit
# endpoints_with_upper_page_limits = {
#     'tags': 20,
#     'creators': 25,
#     ''
# }


def get_random_page_number():
    return random.randint(1,20)

if __name__ == "__main__":
    get_random_page_number()