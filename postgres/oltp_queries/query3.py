import psycopg2
import time
import json
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("table_name")
args = parser.parse_args()

table_name = args.table_name
if table_name not in ('sf1', 'sf2', 'sf3', 'sf4', 'sf5'):
    raise ValueError("Wrong table name")

conn = psycopg2.connect("dbname=twitter user=postgres password=bdma1234")
cur = conn.cursor()

def find_tweets_with_hashtag_and_retweets(cur, table, hashtag, retweet_threshold):
    """ Find tweets containing a specific hashtag with retweet count greater than the threshold. """
    query = f"""
    SELECT data 
    FROM {table} 
    WHERE data -> 'entities' -> 'hashtags' @> '[{{"text": "{hashtag}"}}]'::jsonb
    AND (data ->> 'retweet_count') ~ '^[0-9]+$' 
    AND (data ->> 'retweet_count')::int >= {retweet_threshold};
    """
    cur.execute(query)
    return cur.fetchall()

def find_influencer_accounts(cur, table, follower_threshold):
    """ Find users with followers count greater than the threshold. """
    query = f"""
    SELECT data -> 'user' 
    FROM {table} 
    WHERE (data -> 'user' ->> 'followers_count')::int > {follower_threshold};
    """
    cur.execute(query)
    return cur.fetchall()

# Finding tweets with a specific hashtag and high retweet count
start_time = time.time()
find_tweets_with_hashtag_and_retweets(cur, table_name, 'ThingsICantLiveWithout', 0)    
end_time = time.time()
with open("results/{}.txt".format(table_name), "a") as file:
    file.write(f"Execution Time for OLTP Query 3: {end_time - start_time} s\n")

# Finding influencer accounts
start_time = time.time()
find_influencer_accounts(cur, table_name, 10000)
end_time = time.time()
with open("results/{}.txt".format(table_name), "a") as file:
    file.write(f"Execution Time for OLTP Query 4: {end_time - start_time} s\n")

cur.close()
conn.close()
