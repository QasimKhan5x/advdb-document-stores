import psycopg2
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

def delete_tweets_from_users_with_few_followers(cur, table):
    """ Delete tweets from users with few followers. """
    delete_query = f"""
    DELETE FROM {table}
    WHERE (data->'user'->>'followers_count')::int < 100;
    """
    start_time = time.time()
    cur.execute(delete_query)
    deleted_count = cur.rowcount  # Get the number of rows affected
    end_time = time.time()

    print(f"Time taken to delete tweets from users with few followers: {end_time - start_time} seconds")

    with open("results/{}.txt".format(table_name), "a") as file:
        file.write(f"Execution Time for OLTP Query 8: {end_time - start_time} s\n")

    return deleted_count

few_followers_deleted = delete_tweets_from_users_with_few_followers(cur, table_name)
conn.commit()
cur.close()
conn.close()

print(f"Number of tweets from users with few followers deleted: {few_followers_deleted}")
