import random
import time
from argparse import ArgumentParser

from bson.objectid import ObjectId
from pymongo import MongoClient

parser = ArgumentParser()
parser.add_argument("coll_name")
args = parser.parse_args()

uri = "mongodb://localhost:27017/"
db_name = "advdb_project"
collection_name = args.coll_name
if collection_name not in ("sf1", "sf2", "sf3", "sf4", "sf5"):
    raise ValueError("Wrong collection name")

# Create a MongoDB client (assuming MongoDB is running on the default host and port)
client = MongoClient("localhost", 27017)

# Use a specific database and collection
db = client[db_name]
collection = db[collection_name]

# Define a set of random hashtags
hashtags = ["#Tech", "#News", "#Sports", "#Entertainment", "#Travel"]


# Random data generators for various fields
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


def random_entities():
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
            "_id": ObjectId(),
            "text": "Sample tweet "
            + random.choice(["#Tech", "#News", "#Sports", "#Entertainment", "#Travel"]),
            "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
            "user": random_user(),
            "entities": random_entities(),
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


# How many total documents?
N = collection.count_documents({})
# Generate data for a quarter of them
sample_documents = generate_data(N // 4)

# Insert the documents into the MongoDB collection and measure the execution time
start_time = time.time()
result = collection.insert_many(sample_documents)
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time
print("Execution time for batch insert:", execution_time, "seconds")
