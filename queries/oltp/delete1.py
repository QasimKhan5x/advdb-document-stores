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

def delete_low_engagement_tweets(collection):
    """ Delete tweets with low engagement. """
    start_time = time.time()
    result = collection.delete_many({
        "retweet_count": {"$lte": 5},
        "favorited": False,
        "entities.user_mentions": {"$size": 0}
    })
    end_time = time.time()
    print(f"Time taken to delete low engagement tweets: {end_time - start_time} seconds")
    return result.deleted_count

# Execute the functions
low_engagement_deleted = delete_low_engagement_tweets(collection)

# Print the number of documents deleted for each operation
print(f"Number of low engagement tweets deleted: {low_engagement_deleted}")
