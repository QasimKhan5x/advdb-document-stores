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

def delete_low_engagement_tweets(cur, table):
    """ Delete tweets with low engagement. """
    delete_query = f"""
    DELETE FROM {table}
    WHERE 
      CASE 
        WHEN data->>'retweet_count' ~ '^[0-9]+$' THEN (data->>'retweet_count')::int <= 5
        ELSE FALSE
      END
      AND (data->>'favorited')::boolean = FALSE
      AND jsonb_array_length(data->'entities'->'user_mentions') = 0;
    """
    start_time = time.time()
    cur.execute(delete_query)
    deleted_count = cur.rowcount  # Get the number of rows affected
    end_time = time.time()

    print(f"Time taken to delete low engagement tweets: {end_time - start_time} seconds")

    with open("results/{}.txt".format(table_name), "a") as file:
      file.write(f"Execution Time for OLTP Query 7: {end_time - start_time} s\n")

    return deleted_count

low_engagement_deleted = delete_low_engagement_tweets(cur, table_name)
conn.commit()
cur.close()
conn.close()

print(f"Number of low engagement tweets deleted: {low_engagement_deleted}")
