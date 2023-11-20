from pymongo import MongoClient
import time

# MongoDB connection details
uri = 'mongodb://localhost:27017/'
db_name = 'advdb_project'
collection_name = 'sf1'

# Connect to MongoDB
client = MongoClient(uri)
db = client[db_name]
collection = db[collection_name]

def delete_tweets_from_users_with_few_followers(collection):
    """ Delete tweets from users with few followers. """
    start_time = time.time()
    result = collection.delete_many({
        "user.followers_count": {"$lt": 100}
    })
    end_time = time.time()
    print(f"Time taken to delete tweets from users with few followers: {end_time - start_time} seconds")
    return result.deleted_count
    
few_followers_deleted = delete_tweets_from_users_with_few_followers(collection)
print(f"Number of tweets from users with few followers deleted: {few_followers_deleted}")
