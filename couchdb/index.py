from argparse import ArgumentParser

import requests
from requests.auth import HTTPBasicAuth


def create_index(db_name, index_spec, auth):
    url = f"http://localhost:5984/{db_name}/_index"
    response = requests.post(url, json=index_spec, auth=auth)
    return response.json()


username = "admin"
password = "password"
auth = HTTPBasicAuth(username, password)

indexes = [
    {"index": {"fields": ["created_at"]}, "name": "created_at_index"},
    {"index": {"fields": ["user.location"]}, "name": "user_location_index"},
    {"index": {"fields": ["entities.hashtags.text"]}, "name": "hashtags_text_index"},
    {"index": {"fields": ["text"]}, "name": "text_index"},
    {"index": {"fields": ["user.id_str"]}, "name": "user_id_str_index"},
]

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("db_name")
    args = parser.parse_args()

    db_name = args.db_name
    for index in indexes:
        result = create_index(db_name, index, auth)
        print(result)
