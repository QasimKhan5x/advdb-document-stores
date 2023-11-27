from pymongo import MongoClient
import time
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("coll_name")
args = parser.parse_args()

uri = 'mongodb://localhost:27017/'
db_name = 'advdb_project'
collection_name =  args.coll_name
if collection_name not in ('sf1', 'sf2', 'sf3', 'sf4', 'sf5'):
    raise ValueError("Wrong collection name")

# Connect to MongoDB
client = MongoClient(uri)
db = client[db_name]
collection = db[collection_name]

def update_user_profiles_based_on_activity(collection, tweet_threshold):
    """ Update user profiles who have tweeted more than the threshold. """
    users_to_update = collection.aggregate([
        {"$group": {"_id": "$user.id_str", "tweet_count": {"$sum": 1}}},
        {"$match": {"tweet_count": {"$gt": tweet_threshold}}}
    ])

    for user in users_to_update:
        collection.update_many(
            {"user.id_str": user["_id"]},
            {"$set": {"user.description": user.get("description", "") + " Frequent Tweeter"}}
        )

def tag_users_based_on_topic_interaction(collection, keyword):
    """ Tag users based on interaction with specific keywords or hashtags. """
    users_to_tag = collection.aggregate([
        {"$match": {"$or": [{"text": {"$regex": keyword}}, {"entities.hashtags.text": keyword}]}},
        {"$group": {"_id": "$user.id_str"}}
    ])

    for user in users_to_tag:
        collection.update_many(
            {"user.id_str": user["_id"]},
            {"$set": {"user.description": user.get("description", "") + " Interested in " + keyword}}
        )

# For updating user profiles based on activity
start_time = time.time()
update_user_profiles_based_on_activity(collection, 1)
end_time = time.time()
print("5. update_user_profiles_based_on_activity:", end_time - start_time)

# For tagging users based on interaction with 'technology'
start_time = time.time()
tag_users_based_on_topic_interaction(collection, 'life')
end_time = time.time()
print("6. tag_users_based_on_topic_interaction:", end_time - start_time)