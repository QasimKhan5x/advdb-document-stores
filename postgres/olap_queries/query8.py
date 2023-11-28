import psycopg2
import sys
from argparse import ArgumentParser
import time

parser = ArgumentParser()
parser.add_argument("table_name")
args = parser.parse_args()

table_name = args.table_name
if table_name not in ('sf1', 'sf2', 'sf3', 'sf4', 'sf5'):
    raise ValueError("Wrong table name")

def connect_to_postgres(dbname, user, password, host='localhost', port=5432):
    """Connect to the PostgreSQL database and return the connection and cursor."""
    try:
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        sys.exit(1)

def analyze_tweet_topic_evolution(cur, table_name):
    """Analyze the evolution of tweet topics and engagement over time."""
    query = f"""
    WITH expanded_hashtags AS (
        SELECT
            SUBSTRING(data->>'created_at', 1, 7) AS month,
            jsonb_array_elements(data->'entities'->'hashtags') AS hashtag,
            CASE WHEN data->>'retweet_count' ~ '^[0-9]+$' THEN (data->>'retweet_count')::int ELSE 0 END AS retweet_count,
            CASE WHEN (data->>'favorited')::boolean THEN 1 ELSE 0 END AS favorited
        FROM
            {table_name}
    )
    SELECT
        month,
        hashtag->>'text' AS hashtag_text,
        COUNT(*) AS total_tweets,
        SUM(retweet_count) AS total_retweets,
        SUM(favorited) AS total_favorites
    FROM
        expanded_hashtags
    GROUP BY
        month, hashtag_text
    ORDER BY
        month, total_retweets DESC, total_favorites DESC;
    """
    start_time = time.time()
    cur.execute(query)
    results = cur.fetchall()
    end_time = time.time()
    with open("results/{}.txt".format(table_name), "a") as file:
        file.write(f"Execution Time for OLAP Query 8: {end_time - start_time} s\n")

    return results

if __name__ == "__main__":
    dbname = 'twitter'        
    user = 'postgres'         
    password = 'bdma1234' 

    conn, cur = connect_to_postgres(dbname, user, password)
    results = analyze_tweet_topic_evolution(cur, args.table_name)
    
    for row in results:
        print(row)

    cur.close()
    conn.close()
