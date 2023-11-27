from pymongo import MongoClient
import time
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("coll_name")
args = parser.parse_args()
print(args.coll_name)

uri = 'mongodb://localhost:27017/'
db_name = 'advdb_project'
collection_name =  args.coll_name
if collection_name not in ('sf1', 'sf2', 'sf3', 'sf4', 'sf5'):
    raise ValueError("Wrong collection name")

def connect_to_mongodb(uri, db_name, collection):
    """ Connect to MongoDB and return the specified collection from the database. """
    client = MongoClient(uri)
    return client[db_name][collection]

def find_tweets_with_hashtag_and_retweets(collection, hashtag, retweet_threshold):
    """ Find tweets containing a specific hashtag with retweet count greater than the threshold. """
    query = {
        "entities.hashtags.text": hashtag,
        "retweet_count": {"$gte": retweet_threshold}
    }
    return collection.find(query)

def find_influencer_accounts(collection, follower_threshold):
    """ Find users with followers count greater than the threshold. """
    query = {
        "user.followers_count": {"$gt": follower_threshold}
    }
    return collection.find(query, { "user.id": 1, "_id": 0 })

# Example usage
collection = connect_to_mongodb('mongodb://127.0.0.1:27017', db_name, collection_name)

start_time = time.time()
# For finding tweets with a specific hashtag and high retweet count
print(len(list(find_tweets_with_hashtag_and_retweets(collection, 'ThingsICantLiveWithout', 1))))
end_time = time.time()
print("3. find_tweets_with_hashtag_and_retweets:", end_time - start_time)

start_time = time.time()
# For finding influencer accounts
print(len(list(find_influencer_accounts(collection, 1000))))
end_time = time.time()
print("4. find_influencer_accounts:", end_time - start_time)