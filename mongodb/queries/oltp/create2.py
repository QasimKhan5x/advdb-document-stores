import json
import random
from pprint import pprint
from pymongo import MongoClient
from bson.objectid import ObjectId
import time
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("coll_name")
args = parser.parse_args()

if args.coll_name not in ('sf1', 'sf2', 'sf3', 'sf4', 'sf5'):
    raise ValueError("Wrong collection name")

random.seed(0)

# MongoDB connection (assuming default host and port)
uri = 'mongodb://localhost:27017/'
db_name = 'advdb_project'
collection_name =  args.coll_name
client = MongoClient('localhost', 27017)
db = client[db_name]
collection = db[collection_name]
N = collection.count_documents({})

def sample_bot_users(sample=5):

    # Load the data from the provided file to sample some users
    file_path = '../../data/twitter_sf_1.json'

    # Read the file and load the data into a list of dicts
    users = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            tweet = json.loads(line)
            if 'user' in tweet:
                users.append(tweet['user'])

    # Sample a subset of users to designate as bots
    bot_users = random.sample(users, sample)

    # Update the bot_users with a bot flag
    for user in bot_users:
        user['is_bot'] = True

    return bot_users

# take 4th root of number of documents
bot_users = sample_bot_users(int(N**0.25))


# Random data generators for various fields
def random_entities():
    return {
        "hashtags": [{"text": random.choice(["Tech", "News", "Sports", "Entertainment", "Travel"])} for _ in range(random.randint(0, 3))],
        "urls": [],
        "user_mentions": [{"screen_name": "user" + str(random.randint(1, 100)),
                           "name": "User" + str(random.randint(1, 100)),
                           "id": random.randint(10000, 99999),
                           "id_str": str(random.randint(10000, 99999))} for _ in range(random.randint(0, 2))]
    }

# Data generator function for bot tweets
def generate_bot_tweets(num_tweets, bot_users):
    tweets = []
    for _ in range(num_tweets):
        bot_user = random.choice(bot_users)  # Choose a random bot user
        tweet = {
            "_id": ObjectId(),
            "text": "Bot tweet " + random.choice(["#Tech", "#News", "#Sports", "#Entertainment", "#Travel"]),
            "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
            "user": bot_user,
            "entities": random_entities(),
            "in_reply_to_status_id_str": None if random.choice([True, False]) else str(random.randint(10000, 99999)),
            "coordinates": None,
            "place": None,
            "retweet_count": random.randint(0, 100),
            "favorited": random.choice([True, False]),
            "retweeted": random.choice([True, False])
        }
        tweets.append(tweet)
    return tweets

# Generate tweets posted by bots
num_tweets = N // 4
bot_tweets = generate_bot_tweets(num_tweets, bot_users)

# Batch insert and measure execution time
start_time = time.time()
result = collection.insert_many(bot_tweets)
end_time = time.time()

execution_time = end_time - start_time
print("Execution time for batch insert:", execution_time, "seconds")
