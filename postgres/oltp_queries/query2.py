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

hashtags = ["Tech", "News", "Sports", "Entertainment", "Travel"]

def get_table_size(cur, table):
    query = f"SELECT COUNT(*) FROM {table}"
    cur.execute(query)
    return cur.fetchone()[0]

# Sample a subset of users to designate as bots
def sample_bot_users(sample=5):
    bot_users = []
    for _ in range(sample):
        bot_users.append({
            "id": random.randint(10000, 99999),
            "id_str": str(random.randint(10000, 99999)),
            "name": "BotUser" + str(random.randint(1, 100)),
            "screen_name": "botuser" + str(random.randint(1, 100)),
            "is_bot": True  # flag to indicate it's a bot
        })
    return bot_users
bot_users = sample_bot_users(10)  

# Random data generators for various fields (adjusted for PostgreSQL)
def random_entities():
    return {
        "hashtags": [{"text": random.choice(hashtags)} for _ in range(random.randint(1, 3))],
        "urls": [],
        "user_mentions": []
    }

def generate_bot_tweets(n):
    data = []
    for _ in range(n):
        bot_user = random.choice(bot_users)  # Choose a random bot user
        obj = {
            "text": "Bot tweet #" + random.choice(hashtags),
            "created_at": time.strftime("%a %b %d %H:%M:%S +0000 %Y"),
            "user": bot_user,
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

table_size = get_table_size(cur, table_name)
num_records_to_insert = table_size // 4

# Generate bot tweets
bot_tweets = generate_bot_tweets(num_records_to_insert)  # Adjust the number as needed

query = "INSERT INTO sf1 (data) VALUES " + ",".join(["(%s)"] * len(bot_tweets))
start_time = time.time()
cur.execute(query, bot_tweets)
end_time = time.time()
conn.commit()
cur.close()
conn.close()

with open("results/{}.txt".format(table_name), "a") as file:
    file.write(f"Execution Time for OLTP Query 2: {end_time - start_time} s\n")

print(f"Inserted {num_records_to_insert} records into {table_name}.")