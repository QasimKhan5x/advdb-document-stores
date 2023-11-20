import sys
import time

from pymongo import MongoClient


def connect_to_mongodb(uri, db_name):
    """Connect to MongoDB and return the database object."""
    client = MongoClient(uri)
    return client[db_name]


def determine_influential_users(collection):
    """Determine influential users based on retweets, favorites, and mentions, categorized by follower count tiers."""
    pipeline = [
        {
            "$unwind": {
                "path": "$entities.user_mentions",
                "preserveNullAndEmptyArrays": True,
            }
        },
        {
            "$group": {
                "_id": {
                    "follower_tier": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$lt": ["$user.followers_count", 1000]},
                                    "then": "Tier 1",
                                },
                                {
                                    "case": {
                                        "$and": [
                                            {"$gte": ["$user.followers_count", 1000]},
                                            {"$lte": ["$user.followers_count", 10000]},
                                        ]
                                    },
                                    "then": "Tier 2",
                                },
                            ],
                            "default": "Tier 3",
                        }
                    },
                    "screen_name": "$user.screen_name",
                },
                "total_retweets": {"$sum": "$retweet_count"},
                "favorited_count": {"$sum": {"$cond": ["$favorited", 1, 0]}},
            }
        },
        {
            "$project": {
                "follower_tier": "$_id.follower_tier",
                "screen_name": "$_id.screen_name",
                "engagement_score": {"$add": ["$total_retweets", "$favorited_count"]},
                "mention_count": {"$sum": 1},
            }
        },
        {"$sort": {"engagement_score": -1, "mention_count": -1}},
    ]
    start_time = time.time()
    results = collection.aggregate(pipeline)
    end_time = time.time()

    print(f"1. Execution time: {end_time - start_time} seconds")
    return results


def analyze_content_evolution(collection, specific_words):
    """Analyze content evolution over time with user engagement."""
    start_time = time.time()
    pipeline = [
        {"$match": {"text": {"$regex": "|".join(specific_words)}}},
        {
            "$project": {
                "month": {"$substr": ["$created_at", 0, 7]},
                "words": {"$split": ["$text", " "]},
                "retweet_count": {
                    "$cond": {
                        "if": {"$not": [{"$isNumber": "$retweet_count"}]},
                        "then": 0,
                        "else": "$retweet_count",
                    }
                },
                "favorited": {"$cond": [{"$eq": ["$favorited", True]}, 1, 0]},
                "user_location": "$user.location",
                "user_followers_count": "$user.followers_count",
            }
        },
        {"$unwind": "$words"},
        {"$match": {"words": {"$in": specific_words}}},
        {
            "$group": {
                "_id": {
                    "month": "$month",
                    "word": "$words",
                    "user_location": "$user_location",
                    "user_type": {
                        "$cond": [
                            {"$gt": ["$user_followers_count", 10000]},
                            "Influencer",
                            "Regular User",
                        ]
                    },
                },
                "avg_engagement": {"$avg": {"$add": ["$retweet_count", "$favorited"]}},
                "usage_count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.month": 1, "usage_count": -1, "avg_engagement": -1}},
    ]
    results = collection.aggregate(pipeline)
    end_time = time.time()
    print(f"2. Execution time: {end_time - start_time} seconds")
    return results


def analyze_network_interactions(collection):
    """Analyze network interactions and identify key characteristics of the most interacted-with tweets."""
    pipeline = [
        {"$unwind": "$entities.user_mentions"},
        {
            "$project": {
                "main_user_screen_name": "$user.screen_name",
                "main_user_id": "$user.id",
                "mentioned_user_id": "$entities.user_mentions.id",
                "mentioned_user_screen_name": "$entities.user_mentions.screen_name",
                "tweet_text": "$text",
                "retweet_count": 1,
            }
        },
        {
            "$group": {
                "_id": {
                    "main_user_screen_name": "$main_user_screen_name",
                    "main_user_id": "$main_user_id",
                    "tweet_text": "$tweet_text",
                    "retweet_count": "$retweet_count",
                },
                "unique_mentions_count": {"$sum": 1},
                "avg_followers_of_mentioned_users": {
                    "$avg": "$mentioned_user_id.followers_count"
                },
            }
        },
        {"$sort": {"retweet_count": -1}},
        {"$limit": 100},
    ]
    start_time = time.time()
    results = collection.aggregate(pipeline)
    end_time = time.time()
    print("3. Execution time: {} seconds".format(end_time - start_time))
    return results


def analyze_user_behavior_and_content_strategy(collection):
    """Analyze user behavior and content strategy optimization."""
    pipeline = [
        {"$unwind": {"path": "$entities.hashtags", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$entities.hashtags.text",
                "total_tweets": {"$sum": 1},
                "total_retweets": {"$sum": "$retweet_count"},
                "total_favorites": {
                    "$sum": {"$cond": [{"$eq": ["$favorited", True]}, 1, 0]}
                },
            }
        },
    ]
    start_time = time.time()
    results = collection.aggregate(pipeline)
    end_time = time.time()
    print("4. Execution time: {} seconds".format(end_time - start_time))
    return list(results)


def analyze_user_activity_patterns(collection):
    """Analyze patterns of users' activity over time, including peak activity hours."""
    pipeline = [
        {
            "$project": {
                "hour_of_day": {"$hour": {"$toDate": "$created_at"}},
                "user_location": "$user.location",
                "hashtag": "$entities.hashtags.text",
            }
        },
        {"$unwind": {"path": "$hashtag", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": {
                    "hour_of_day": "$hour_of_day",
                    "user_location": "$user_location",
                    "hashtag": "$hashtag",
                },
                "tweet_count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.hour_of_day": 1, "_id.user_location": 1, "_id.hashtag": 1}},
    ]

    start_time = time.time()
    results = collection.aggregate(pipeline)
    end_time = time.time()
    print("5. Execution time: {} seconds".format(end_time - start_time))

    return list(results)


def analyze_twitter_influencers(collection):
    """Analyze key influencers in the Twitter network based on interactions."""
    pipeline = [
        {
            "$project": {
                "original_user_screen_name": "$user.screen_name",
                "retweet_id": "$retweeted_status.id",
                "mentions": "$entities.user_mentions",
                "retweet_count": 1,
            }
        },
        {"$unwind": {"path": "$mentions", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": "$original_user_screen_name",
                "total_retweets": {"$sum": "$retweet_count"},
                "total_mentions": {"$sum": 1},
                "cumulative_followers_of_mentioned_users": {
                    "$sum": "$mentions.followers_count"
                },
            }
        },
        {
            "$sort": {
                "total_retweets": -1,
                "total_mentions": -1,
                "cumulative_followers_of_mentioned_users": -1,
            }
        },
    ]

    start_time = time.time()
    results = collection.aggregate(pipeline)
    end_time = time.time()
    print("6. Execution time: {} seconds".format(end_time - start_time))

    return list(results)


def analyze_geographical_demographic_insights(collection):
    """Analyze geographical and demographic insights from tweet patterns."""
    pipeline = [
        {"$match": {"user.location": {"$nin": [None, ""]}}},
        {
            "$project": {
                "user_location": "$user.location",
                "hashtags": "$entities.hashtags",
                "retweet_count": 1,
                "favorited": 1,
            }
        },
        {"$unwind": {"path": "$hashtags", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": {"user_location": "$user_location", "hashtag": "$hashtags.text"},
                "total_tweets": {"$sum": 1},
                "total_retweets": {"$sum": "$retweet_count"},
                "total_favorites": {
                    "$sum": {"$cond": [{"$eq": ["$favorited", True]}, 1, 0]}
                },
            }
        },
        {
            "$sort": {
                "_id.user_location": 1,
                "total_retweets": -1,
                "total_favorites": -1,
            }
        },
    ]

    start_time = time.time()
    results = collection.aggregate(pipeline)
    end_time = time.time()
    print("7. Execution time: {} seconds".format(end_time - start_time))

    return list(results)


def analyze_tweet_topic_evolution(collection):
    """Analyze the evolution of tweet topics and engagement over time."""
    pipeline = [
        {
            "$project": {
                "month": {"$substr": ["$created_at", 0, 7]},
                "hashtags": "$entities.hashtags",
                "retweet_count": 1,
                "favorited": 1,
            }
        },
        {"$unwind": {"path": "$hashtags", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": {"month": "$month", "hashtag": "$hashtags.text"},
                "total_tweets": {"$sum": 1},
                "total_retweets": {"$sum": "$retweet_count"},
                "total_favorites": {
                    "$sum": {"$cond": [{"$eq": ["$favorited", True]}, 1, 0]}
                },
            }
        },
        {"$sort": {"_id.month": 1, "total_retweets": -1, "total_favorites": -1}},
    ]

    start_time = time.time()
    results = collection.aggregate(pipeline)
    end_time = time.time()
    print("8. Execution time: {} seconds".format(end_time - start_time))

    return list(results)


# Main execution
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <collection_name>")
        sys.exit(1)

    collection_name = sys.argv[1]
    db = connect_to_mongodb("mongodb://localhost:27017/", "advdb_project")
    determine_influential_users(db[collection_name])
    analyze_content_evolution(db[collection_name], ["twitter", "love"])
    analyze_network_interactions(db[collection_name])
    analyze_user_behavior_and_content_strategy(db[collection_name])
    analyze_user_activity_patterns(db[collection_name])
    analyze_twitter_influencers(db[collection_name])
    analyze_geographical_demographic_insights(db[collection_name])
    analyze_tweet_topic_evolution(db[collection_name])
