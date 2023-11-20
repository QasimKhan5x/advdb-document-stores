from pymongo import MongoClient
import time

# MongoDB connection details (replace with your own details)
uri = 'mongodb://localhost:27017/'
db_name = 'advdb_project'
collection_name = 'sf1'

# Connect to MongoDB
client = MongoClient(uri)
db = client[db_name]
collection = db[collection_name]

def update_user_profiles_based_on_activity(db, tweet_threshold):
    """ Update user profiles who have tweeted more than the threshold. """
    users_to_update = db.tweets.aggregate([
        {"$group": {"_id": "$user.id_str", "tweet_count": {"$sum": 1}}},
        {"$match": {"tweet_count": {"$gt": tweet_threshold}}}
    ])

    for user in users_to_update:
        db.tweets.update_many(
            {"user.id_str": user["_id"]},
            {"$set": {"user.description": user.get("description", "") + " Frequent Tweeter"}}
        )

def tag_users_based_on_topic_interaction(db, keyword):
    """ Tag users based on interaction with specific keywords or hashtags. """
    users_to_tag = db.tweets.aggregate([
        {"$match": {"$or": [{"text": {"$regex": keyword}}, {"entities.hashtags.text": keyword}]}},
        {"$group": {"_id": "$user.id_str"}}
    ])

    for user in users_to_tag:
        db.tweets.update_many(
            {"user.id_str": user["_id"]},
            {"$set": {"user.description": user.get("description", "") + " Interested in " + keyword}}
        )

# For updating user profiles based on activity
start_time = time.time()
update_user_profiles_based_on_activity(db, 1)
end_time = time.time()
print("5. update_user_profiles_based_on_activity:", end_time - start_time)

# For tagging users based on interaction with 'technology'
start_time = time.time()
tag_users_based_on_topic_interaction(db, 'life')
end_time = time.time()
print("6. tag_users_based_on_topic_interaction:", end_time - start_time)
