import json
import psycopg2
import random
import time
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("table_name")
args = parser.parse_args()

table_name = args.table_name
if table_name not in ('sf1', 'sf2', 'sf3', 'sf4', 'sf5'):
    raise ValueError("Wrong table name")

conn = psycopg2.connect("dbname=twitter user=postgres password=bdma1234")
cur = conn.cursor()

def get_table_size(cur, table):
    query = f"SELECT COUNT(*) FROM {table}"
    cur.execute(query)
    return cur.fetchone()[0]

# Define a set of random hashtags
hashtags = ["Tech", "News", "Sports", "Entertainment", "Travel"]

# Random data generators for various fields 
def random_user():
    return {
        "id": random.randint(10000, 99999),
        "id_str": str(random.randint(10000, 99999)),
        "name": "User" + str(random.randint(1, 100)),
        "screen_name": "user" + str(random.randint(1, 100)),
        "location": random.choice(["New York", "London", "Tokyo", None]),
        "description": random.choice(["Love tech and music", "Travel enthusiast", "Sports fan", None]),
        "followers_count": random.randint(0, 1000),
        "friends_count": random.randint(0, 500),
        "listed_count": random.randint(0, 100),
        "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
        "favourites_count": random.randint(0, 200),
        "utc_offset": random.choice([None, 3600, -3600]),
        "time_zone": random.choice(["EST", "PST", "GMT", None]),
        "geo_enabled": random.choice([True, False]),
        "verified": random.choice([True, False]),
        "statuses_count": random.randint(100, 10000)
    }

def random_entities():
    return {
        "hashtags": [{"text": random.choice(hashtags)} for _ in range(random.randint(1, 3))],
        "urls": [],
        "user_mentions": []
    }

def generate_data(n):
    data = []
    for _ in range(n):
        obj = {
            "text": "Sample tweet #" + random.choice(hashtags),
            "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
            "user": random_user(),
            "entities": random_entities(),
            "in_reply_to_status_id_str": None,
            "coordinates": None,
            "place": None,
            "retweet_count": random.randint(0, 100),
            "favorited": random.choice([True, False]),
            "retweeted": random.choice([True, False])
        }
        data.append(json.dumps(obj))
    return data

# Get the size of the table and determine the number of records to insert
table_size = get_table_size(cur, table_name)
num_records_to_insert = table_size // 4

# Generate sample tweets
sample_tweets = generate_data(num_records_to_insert)

# Prepare the INSERT query using the table name from the argument
query = f"INSERT INTO {table_name} (data) VALUES " + ",".join(["(%s)"] * len(sample_tweets))

start_time = time.time()
cur.execute(query, sample_tweets)
end_time = time.time()
conn.commit()
cur.close()
conn.close()

with open("results/{}.txt".format(table_name), "a") as file:
    file.write(f"Execution Time for OLTP Query 1: {end_time - start_time} s\n")

print(f"Inserted {num_records_to_insert} records into {table_name}.")