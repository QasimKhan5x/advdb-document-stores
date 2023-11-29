# go here for relevant info
# https://chat.openai.com/c/93f89a3a-f904-4e71-be2b-c99b561774e7

import argparse
import json
import random
import time
import uuid
from pprint import pprint

import requests
from requests.auth import HTTPBasicAuth


def get_document_count(db_name, auth):
    response = requests.get(f"http://localhost:5984/{db_name}", auth=auth)
    return response.json()["doc_count"]


# read 1
def find_tweets_with_hashtag_and_retweets(db_name, hashtag, retweet_threshold, auth):
    query = {
        "selector": {
            "text": {
                "$regex": hashtag,
            },
            "retweet_count": {"$gte": retweet_threshold},
        }
    }
    start_time = time.time()
    response = requests.post(
        f"http://localhost:5984/{db_name}/_find", json=query, auth=auth
    )
    print(f"read 1: {time.time() - start_time} seconds")
    return response.json()


# read 2
def find_influencer_accounts(db_name, follower_threshold, auth):
    query = {
        "selector": {"user.followers_count": {"$gt": follower_threshold}},
        "fields": ["user.id"],
    }
    start_time = time.time()
    response = requests.post(
        f"http://localhost:5984/{db_name}/_find", json=query, auth=auth
    )
    print(f"read 2: {time.time() - start_time} seconds")
    return response.json()


# create 1
def random_user():
    return {
        "id": random.randint(10000, 99999),
        "id_str": str(random.randint(10000, 99999)),
        "name": "User" + str(random.randint(1, 100)),
        "screen_name": "user" + str(random.randint(1, 100)),
        "location": random.choice(["New York", "London", "Tokyo", None]),
        "description": random.choice(
            ["Love tech and music", "Travel enthusiast", "Sports fan", None]
        ),
        "followers_count": random.randint(0, 1000),
        "friends_count": random.randint(0, 500),
        "listed_count": random.randint(0, 100),
        "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
        "favourites_count": random.randint(0, 200),
        "utc_offset": random.choice([None, 3600, -3600]),
        "time_zone": random.choice(["EST", "PST", "GMT", None]),
        "geo_enabled": random.choice([True, False]),
        "verified": random.choice([True, False]),
        "statuses_count": random.randint(100, 10000),
    }


def random_entities1():
    return {
        "hashtags": [
            {
                "text": random.choice(
                    ["Tech", "News", "Sports", "Entertainment", "Travel"]
                )
            }
            for _ in range(random.randint(0, 3))
        ],
        "urls": [],
        "user_mentions": [
            {
                "screen_name": "user" + str(random.randint(1, 100)),
                "name": "User" + str(random.randint(1, 100)),
                "id": random.randint(10000, 99999),
                "id_str": str(random.randint(10000, 99999)),
            }
            for _ in range(random.randint(0, 2))
        ],
    }


def generate_data(n):
    data = []
    for _ in range(n):
        obj = {
            "_id": str(uuid.uuid4()),
            "text": "Sample tweet "
            + random.choice(["#Tech", "#News", "#Sports", "#Entertainment", "#Travel"]),
            "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
            "user": random_user(),
            "entities": random_entities1(),
            "in_reply_to_status_id_str": None
            if random.choice([True, False])
            else str(random.randint(10000, 99999)),
            "coordinates": None,
            "place": None,
            "retweet_count": random.randint(0, 100),
            "favorited": random.choice([True, False]),
            "retweeted": random.choice([True, False]),
        }
        data.append(obj)
    return data


# create 2
def sample_bot_users(sample=5):
    # Load the data from the provided file to sample some users
    file_path = "data/twitter_sf_1.json"

    # Read the file and load the data into a list of dicts
    users = []
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            tweet = json.loads(line)
            if "user" in tweet:
                users.append(tweet["user"])

    # Sample a subset of users to designate as bots
    bot_users = random.sample(users, sample)

    # Update the bot_users with a bot flag
    for user in bot_users:
        user["is_bot"] = True

    return bot_users


def random_entities2():
    return {
        "hashtags": [
            {
                "text": random.choice(
                    ["Tech", "News", "Sports", "Entertainment", "Travel"]
                )
            }
            for _ in range(random.randint(0, 3))
        ],
        "urls": [],
        "user_mentions": [
            {
                "screen_name": "user" + str(random.randint(1, 100)),
                "name": "User" + str(random.randint(1, 100)),
                "id": random.randint(10000, 99999),
                "id_str": str(random.randint(10000, 99999)),
            }
            for _ in range(random.randint(0, 2))
        ],
    }


def generate_bot_tweets(num_tweets, bot_users):
    tweets = []
    for _ in range(num_tweets):
        bot_user = random.choice(bot_users)  # Choose a random bot user
        tweet = {
            "_id": str(uuid.uuid4()),
            "text": "Bot tweet "
            + random.choice(["#Tech", "#News", "#Sports", "#Entertainment", "#Travel"]),
            "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
            "user": bot_user,
            "entities": random_entities2(),
            "in_reply_to_status_id_str": None
            if random.choice([True, False])
            else str(random.randint(10000, 99999)),
            "coordinates": None,
            "place": None,
            "retweet_count": random.randint(0, 100),
            "favorited": random.choice([True, False]),
            "retweeted": random.choice([True, False]),
        }
        tweets.append(tweet)
    return tweets


def insert_documents(db_name, documents, auth):
    bulk_docs = {"docs": documents}
    response = requests.post(
        f"http://localhost:5984/{db_name}/_bulk_docs", json=bulk_docs, auth=auth
    )
    # if response.status_code in [200, 201, 202]:
    #     print("Documents inserted successfully.")
    # else:
    #     print(f"Failed to insert documents. Error: {response.text}")


def delete_low_engagement_tweets(db_name, auth):
    # Find tweets with low engagement
    query = {
        "selector": {
            "retweet_count": {"$lte": 5},
            "favorited": False,
            "entities.user_mentions": {"$size": 0},
        },
        "fields": ["_id", "_rev"],
    }
    find_url = f"http://localhost:5984/{db_name}/_find"
    find_response = requests.post(find_url, json=query, auth=auth)
    tweets_to_delete = find_response.json()["docs"]

    # Prepare documents for deletion
    for tweet in tweets_to_delete:
        tweet["_deleted"] = True

    # Batch delete the documents
    if tweets_to_delete:
        bulk_update_url = f"http://localhost:5984/{db_name}/_bulk_docs"
        bulk_docs = {"docs": tweets_to_delete}
        bulk_delete_response = requests.post(bulk_update_url, json=bulk_docs, auth=auth)
        return len(tweets_to_delete), bulk_delete_response.json()
    return 0, None


def delete_tweets_from_users_with_few_followers(db_name, auth):
    # Find tweets from users with few followers
    query = {
        "selector": {"user.followers_count": {"$lt": 100}},
        "fields": ["_id", "_rev"],
    }
    find_url = f"http://localhost:5984/{db_name}/_find"
    find_response = requests.post(find_url, json=query, auth=auth)
    tweets_to_delete = find_response.json()["docs"]

    # Prepare documents for deletion
    for tweet in tweets_to_delete:
        tweet["_deleted"] = True

    # Batch delete the documents
    if tweets_to_delete:
        bulk_update_url = f"http://localhost:5984/{db_name}/_bulk_docs"
        bulk_docs = {"docs": tweets_to_delete}
        bulk_delete_response = requests.post(bulk_update_url, json=bulk_docs, auth=auth)
        return len(tweets_to_delete), bulk_delete_response.json()
    return 0, None


def update_user_profiles_based_on_activity(db_name, tweet_threshold, auth):
    # Step 1: Fetch the view results
    view_url = f"http://localhost:5984/{db_name}/_design/tweet_analysis/_view/tweet_count_by_user"
    params = {"reduce": "false"}
    view_response = requests.get(view_url, params=params, auth=auth)
    users_data = view_response.json()

    # Collect documents to update
    documents_to_update = []

    # Step 2: Iterate over users and update if tweet_count is above the threshold
    for row in users_data["rows"]:
        if row["value"] > tweet_threshold:
            user_id = row["key"]
            # Fetch user's documents
            user_docs_url = f'http://localhost:5984/{db_name}/_design/user_docs/_view/by_user_id?key="{user_id}"'
            user_docs_response = requests.get(user_docs_url, auth=auth)
            user_docs = user_docs_response.json()

            # Prepare documents for batch update
            for doc in user_docs["rows"]:
                doc_data = doc["value"]
                doc_data["user"]["description"] = (
                    doc_data["user"].get("description", " ") + " Frequent Tweeter"
                )
                documents_to_update.append(doc_data)

    # Batch update the documents
    if documents_to_update:
        bulk_update_url = f"http://localhost:5984/{db_name}/_bulk_docs"
        for i in range(0, len(documents_to_update), 10000):
            bulk_docs = {"docs": documents_to_update[i : i + 10000]}
            bulk_update_response = requests.post(
                bulk_update_url, json=bulk_docs, auth=auth
            )
            # if bulk_update_response.status_code in [200, 201, 202]:
            #     print("Documents updated successfully.")
            #     pprint(bulk_update_response.json())


def fetch_documents_in_batches(db_name, user_id, auth, batch_size=100):
    documents = []
    skip = 0
    while True:
        user_docs_url = f'http://localhost:5984/{db_name}/_design/user_docs/_view/by_user_id?key="{user_id}"&limit={batch_size}&skip={skip}'
        user_docs_response = requests.get(user_docs_url, auth=auth)
        user_docs = user_docs_response.json()
        print(user_docs)
        # Check if there are no more rows
        if not user_docs["rows"]:
            break

        documents.extend(user_docs["rows"])
        skip += batch_size
    return documents


def tag_users_based_on_topic_interaction(db_name, keyword, auth):
    # Step 1: Find all relevant documents
    query = {
        "selector": {
            "$or": [
                {"text": {"$regex": f"(?i){keyword}"}},
                {"entities.hashtags.text": keyword},
            ]
        },
        "fields": ["user.id_str"],
    }
    find_url = f"http://localhost:5984/{db_name}/_find"
    find_response = requests.post(find_url, json=query, auth=auth)
    find_data = find_response.json()

    # Extract unique user IDs
    user_ids = set(
        doc["user"]["id_str"]
        for doc in find_data["docs"]
        if "user" in doc and "id_str" in doc["user"]
    )

    # Step 2: Update each user's documents
    documents_to_update = []
    for user_id in user_ids:
        user_docs = fetch_documents_in_batches(db_name, user_id, auth)
        # Prepare documents for batch update
        for doc in user_docs:
            doc_data = doc["value"]
            doc_data["user"]["description"] = (
                doc_data["user"].get("description", "") + " Interested in " + keyword
            )
            documents_to_update.append(doc_data)

    # Batch update the documents
    if documents_to_update:
        bulk_update_url = f"http://localhost:5984/{db_name}/_bulk_docs"
        for i in range(0, len(documents_to_update), 10000):
            bulk_docs = {"docs": documents_to_update[i : i + 10000]}
            bulk_docs = {"docs": documents_to_update[i : i + 100000]}
            bulk_update_response = requests.post(
                bulk_update_url, json=bulk_docs, auth=auth
            )
            # if bulk_update_response.status_code in [200, 201, 202]:
            #     print("Documents updated successfully.")
            #     pprint(bulk_update_response.json())


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

    username = "admin"
    password = "password"
    auth = HTTPBasicAuth(username, password)
    N = get_document_count(db_name, auth)
    print(f"Number of documents in {db_name}: {N}")

    # read 1
    # response = find_tweets_with_hashtag_and_retweets(
    # db_name, "ThingsICantLiveWithout", 1, auth
    # )

    # read 2
    # response = find_influencer_accounts(db_name, 1000, auth)

    # create 1
    sample_documents = generate_data(N // 4)
    start_time = time.time()
    # for i in range(0, len(sample_documents), 10000):
    # insert_documents(db_name, sample_documents[i : i + 10000], auth)
    print(f"create 1: {time.time() - start_time} seconds")
    N = get_document_count(db_name, auth)
    # print(f"Number of documents in {db_name}: {N}")

    # create 2
    bot_users = sample_bot_users(int(N**0.25))
    bot_tweets = generate_bot_tweets(N // 4, bot_users)
    start_time = time.time()
    # for i in range(0, len(sample_documents), 10000):
    # insert_documents(db_name, bot_tweets[i : i + 10000], auth)
    print(f"create 2: {time.time() - start_time} seconds")
    # N = get_document_count(db_name, auth)
    # print(f"Number of documents in {db_name}: {N}")

    # delete 1
    start_time = time.time()
    # num_deleted, response = delete_low_engagement_tweets(db_name, auth)
    print(f"delete 1: {time.time() - start_time} seconds")

    # delete 2
    start_time = time.time()
    # num_deleted, response = delete_tweets_from_users_with_few_followers(db_name, auth)
    print(f"delete 2: {time.time() - start_time} seconds")

    # update 1
    start_time = time.time()
    update_user_profiles_based_on_activity(db_name, 10, auth)
    print(f"update 1: {time.time() - start_time} seconds")

    # update 2
    start_time = time.time()
    tag_users_based_on_topic_interaction(db_name, "love", auth)
    print(f"update 2: {time.time() - start_time} seconds")


if __name__ == "__main__":
    main()
