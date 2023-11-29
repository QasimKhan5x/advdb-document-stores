from argparse import ArgumentParser

import requests
from requests.auth import HTTPBasicAuth

username = "admin"
password = "password"
auth = HTTPBasicAuth(username, password)

tweet_analysis_doc = {
    "_id": "_design/tweet_analysis",
    "views": {
        "tweet_count_by_user": {
            "map": "function(doc) { if (doc.user && doc.user.id_str) { emit(doc.user.id_str, 1); } }",
            "reduce": "_count",
        }
    },
}

user_docs = {
    "_id": "_design/user_docs",
    "views": {
        "by_user_id": {
            "map": "function(doc) { if (doc.user && doc.user.id_str) { emit(doc.user.id_str, doc); } }"
        }
    },
}

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("db_name")
    args = parser.parse_args()
    db_name = args.db_name

    response = requests.put(
        f'http://localhost:5984/{db_name}/{tweet_analysis_doc["_id"]}',
        json=tweet_analysis_doc,
        auth=auth,
    )
    print(response.json())
    requests.put(
        f'http://localhost:5984/{db_name}/{user_docs["_id"]}',
        json=user_docs,
        auth=auth,
    )
    print(response.json())
