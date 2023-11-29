import argparse
import json
import time

import requests
from requests.auth import HTTPBasicAuth


def create_database(db_name, auth):
    # Check if the database exists
    get_response = requests.get(f"http://localhost:5984/{db_name}", auth=auth)
    if get_response.status_code == 200:
        # Database exists, delete it
        del_response = requests.delete(f"http://localhost:5984/{db_name}", auth=auth)
        if del_response.status_code in [200, 202]:
            print(f"Database '{db_name}' deleted successfully.")
        else:
            print(
                f"Failed to delete existing database '{db_name}'. Error: {del_response.text}"
            )
            return

    # Create new database
    create_response = requests.put(f"http://localhost:5984/{db_name}", auth=auth)
    if create_response.status_code == 201:
        print(f"Database '{db_name}' created successfully.")
    else:
        print(f"Failed to create database '{db_name}'. Error: {create_response.text}")


def insert_documents(db_name, documents, auth):
    bulk_docs = {"docs": documents}
    response = requests.post(
        f"http://localhost:5984/{db_name}/_bulk_docs", json=bulk_docs, auth=auth
    )
    # if response.status_code == 201:
        # print("Documents inserted successfully.")
    # else:
        # print(f"Failed to insert documents. Error: {response.text}")


def main():
    parser = argparse.ArgumentParser(
        description="Insert documents into a CouchDB database."
    )
    parser.add_argument(
        "db_name",
        help="The name of the CouchDB database",
        choices=["sf1", "sf2", "sf3", "sf4", "sf5"],
    )
    args = parser.parse_args()

    db_name = args.db_name
    json_file_path = f"data/twitter_sf_{db_name[-1]}.json"

    username = "admin"
    password = "password"
    auth = HTTPBasicAuth(username, password)

    start_time = time.time()
    create_database(db_name, auth)
    try:
        with open(json_file_path, "r", encoding="utf-8") as file:
            documents = []
            for line in file:
                document = json.loads(line)
                document["_id"] = document["_id"]["$oid"]
                documents.append(document)
                if len(documents) >= 10000:
                    insert_documents(db_name, documents, auth)
                    documents = []
            # Insert any remaining documents
            if documents:
                insert_documents(db_name, documents, auth)
    except Exception as e:
        print(f"An error occurred: {e}")
    else:
        print(f"Load time: {time.time() - start_time}")
    finally:
        # print the total number of documents inserted
        response = requests.get(f"http://localhost:5984/{db_name}", auth=auth)
        if response.status_code == 200:
            print(f"Total documents inserted: {response.json()['doc_count']}")


if __name__ == "__main__":
    main()
