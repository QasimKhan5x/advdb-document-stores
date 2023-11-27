import json
import random
import time
from argparse import ArgumentParser

from bson.objectid import ObjectId
from pymongo import MongoClient

parser = ArgumentParser()
parser.add_argument("coll_name")
args = parser.parse_args()

uri = "mongodb://localhost:27017/"
db_name = "twitter"
collection_name = args.coll_name
if collection_name not in ("sf1", "sf2", "sf3", "sf4", "sf5"):
    raise ValueError("Wrong collection name")


def connect_to_mongodb(uri, db_name, collection):
    """Connect to MongoDB and return the specified collection from the database."""
    client = MongoClient(uri)
    return client[db_name][collection]


def find_tweets_with_hashtag_and_retweets(collection, hashtag, retweet_threshold):
    """Find tweets containing a specific hashtag with retweet count greater than the threshold."""
    query = {
        "entities.hashtags.text": hashtag,
        "retweet_count": {"$gte": retweet_threshold},
    }
    return collection.find(query)


def find_influencer_accounts(collection, follower_threshold):
    """Find users with followers count greater than the threshold."""
    query = {"user.followers_count": {"$gt": follower_threshold}}
    return collection.find(query, {"user.id": 1, "_id": 0})


collection = connect_to_mongodb(uri, db_name, collection_name)

start_time = time.time()
# For finding tweets with a specific hashtag and high retweet count
find_tweets_with_hashtag_and_retweets(collection, "ThingsICantLiveWithout", 1)
end_time = time.time()
print("read 1:", end_time - start_time)

start_time = time.time()
# For finding influencer accounts
find_influencer_accounts(collection, 1000)
end_time = time.time()
print("read 2:", end_time - start_time)

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
            "_id": ObjectId(),
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


# How many total documents?
N = collection.count_documents({})
# Generate data for a quarter of them
sample_documents = generate_data(N // 4)
# Insert the documents into the MongoDB collection and measure the execution time
start_time = time.time()
for i in range(0, len(sample_documents), 250000):
    collection.insert_many(sample_documents[i : i + 250000])
end_time = time.time()
# Calculate the execution time
execution_time = end_time - start_time
print("create 1:", execution_time, "seconds")


def sample_bot_users(sample=5):
    # Load the data from the provided file to sample some users
    file_path = "../../data/twitter_sf_1.json"

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


# take 4th root of number of documents
bot_users = sample_bot_users(int(N**0.25))


# Random data generators for various fields
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


# Data generator function for bot tweets
def generate_bot_tweets(num_tweets, bot_users):
    tweets = []
    for _ in range(num_tweets):
        bot_user = random.choice(bot_users)  # Choose a random bot user
        tweet = {
            "_id": ObjectId(),
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


# Generate tweets posted by bots
num_tweets = N // 4
bot_tweets = generate_bot_tweets(num_tweets, bot_users)
# Batch insert and measure execution time
start_time = time.time()
for i in range(0, len(bot_tweets), 250000):
    collection.insert_many(bot_tweets[i : i + 250000])
end_time = time.time()
execution_time = end_time - start_time
print("create 2:", execution_time, "seconds")


def delete_low_engagement_tweets(collection):
    """Delete tweets with low engagement."""
    start_time = time.time()
    result = collection.delete_many(
        {
            "retweet_count": {"$lte": 5},
            "favorited": False,
            "entities.user_mentions": {"$size": 0},
        }
    )
    end_time = time.time()
    print(f"Delete 1: {end_time - start_time} seconds")
    return result.deleted_count


delete_low_engagement_tweets(collection)


def delete_tweets_from_users_with_few_followers(collection):
    """Delete tweets from users with few followers."""
    start_time = time.time()
    result = collection.delete_many({"user.followers_count": {"$lt": 100}})
    end_time = time.time()
    print(f"Delete 2: {end_time - start_time} seconds")
    return result.deleted_count


delete_tweets_from_users_with_few_followers(collection)


def update_user_profiles_based_on_activity(collection, tweet_threshold):
    """Update user profiles who have tweeted more than the threshold."""
    users_to_update = collection.aggregate(
        [
            {"$group": {"_id": "$user.id_str", "tweet_count": {"$sum": 1}}},
            {"$match": {"tweet_count": {"$gt": tweet_threshold}}},
        ]
    )
    start_time = time.time()
    for user in users_to_update:
        collection.update_many(
            {"user.id_str": user["_id"]},
            {
                "$set": {
                    "user.description": user.get("description", "")
                    + " Frequent Tweeter"
                }
            },
        )
    end_time = time.time()
    print("update 1:", end_time - start_time)


def tag_users_based_on_topic_interaction(collection, keyword):
    """Tag users based on interaction with specific keywords or hashtags."""
    users_to_tag = collection.aggregate(
        [
            {
                "$match": {
                    "$or": [
                        {"text": {"$regex": keyword}},
                        {"entities.hashtags.text": keyword},
                    ]
                }
            },
            {"$group": {"_id": "$user.id_str"}},
        ]
    )
    start_time = time.time()
    for user in users_to_tag:
        collection.update_many(
            {"user.id_str": user["_id"]},
            {
                "$set": {
                    "user.description": user.get("description", "")
                    + " Interested in "
                    + keyword
                }
            },
        )
    end_time = time.time()
    print("update 2:", end_time - start_time)


# For updating user profiles based on activity
update_user_profiles_based_on_activity(collection, 10)

# For tagging users based on interaction with 'life'
tag_users_based_on_topic_interaction(collection, "twitter|life")
