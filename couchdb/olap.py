import argparse
import json
import random
import time
import uuid
from pprint import pprint

import requests
from requests.auth import HTTPBasicAuth


def determine_influential_users(db_name, auth):
    start_time = time.time()
    view_url = f"http://localhost:5984/{db_name}/_design/influential_users/_view/by_tier_and_name?group_level=2"
    response = requests.get(view_url, auth=auth)
    data = response.json()
    pprint(data)

    results = []
    for row in data["rows"]:
        total_retweets = row["value"]["total_retweets"]
        total_retweets = total_retweets if total_retweets else 0
        engagement_score = total_retweets + row["value"]["favorited_count"]
        results.append(
            {
                "follower_tier": row["key"][0],
                "screen_name": row["key"][1],
                "engagement_score": engagement_score,
                "mention_count": row["value"]["mention_count"],
            }
        )

    results.sort(key=lambda x: (-x["engagement_score"], -x["mention_count"]))
    end_time = time.time()
    print("determine_influential_users:", end_time - start_time, "seconds")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("db_name")
    args = parser.parse_args()
    db_name = args.db_name

    username = "admin"
    password = "password"
    auth = HTTPBasicAuth(username, password)

    determine_influential_users(db_name, auth)
