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

def analyze_network_interactions(cur, table_name):
    """Analyze network interactions and identify key characteristics of the most interacted-with tweets."""
    query = f"""
    WITH expanded_mentions AS (
        SELECT
            data->'user'->>'screen_name' AS main_user_screen_name,
            data->'user'->>'id' AS main_user_id,
            mention->>'id' AS mentioned_user_id,
            mention->>'screen_name' AS mentioned_user_screen_name,
            data->>'text' AS tweet_text,
            CASE WHEN data->>'retweet_count' ~ '^[0-9]+$' THEN (data->>'retweet_count')::int ELSE 0 END AS retweet_count,
            CASE WHEN mention->>'followers_count' ~ '^[0-9]+$' THEN (mention->>'followers_count')::int ELSE 0 END AS followers_count
        FROM
            {table_name},
            LATERAL jsonb_array_elements(data->'entities'->'user_mentions') AS mention
    )
    SELECT
        main_user_screen_name,
        main_user_id,
        tweet_text,
        retweet_count,
        COUNT(*) AS unique_mentions_count,
        AVG(followers_count) AS avg_followers_of_mentioned_users
    FROM
        expanded_mentions
    GROUP BY
        main_user_screen_name, main_user_id, tweet_text, retweet_count
    ORDER BY
        retweet_count DESC
    LIMIT 100;
    """
    start_time = time.time()
    cur.execute(query)
    results = cur.fetchall()
    end_time = time.time()
    with open("results/{}.txt".format(table_name), "a") as file:
        file.write(f"Execution Time for OLAP Query 3: {end_time - start_time} s\n")

    return results


if __name__ == "__main__":
    dbname = 'twitter'        
    user = 'postgres'         
    password = 'bdma1234' 

    conn, cur = connect_to_postgres(dbname, user, password)
    results = analyze_network_interactions(cur, args.table_name)
    
    for row in results:
        print(row)

    cur.close()
    conn.close()
