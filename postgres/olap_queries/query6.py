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

def analyze_twitter_influencers(cur, table_name):
    """Analyze key influencers in the Twitter network based on interactions."""
    query = f"""
    WITH expanded_mentions AS (
        SELECT
            data->'user'->>'screen_name' AS original_user_screen_name,
            CASE WHEN data->>'retweet_count' ~ '^[0-9]+$' THEN (data->>'retweet_count')::int ELSE 0 END AS retweet_count,
            mention->>'id' AS mentioned_user_id,
            CASE WHEN mention->>'followers_count' ~ '^[0-9]+$' THEN (mention->>'followers_count')::int ELSE 0 END AS mentioned_user_followers_count
        FROM
            {table_name},
            LATERAL jsonb_array_elements(data->'entities'->'user_mentions') AS mention
    )
    SELECT
        original_user_screen_name,
        SUM(retweet_count) AS total_retweets,
        COUNT(*) AS total_mentions,
        SUM(mentioned_user_followers_count) AS cumulative_followers_of_mentioned_users
    FROM
        expanded_mentions
    GROUP BY
        original_user_screen_name
    ORDER BY
        total_retweets DESC,
        total_mentions DESC,
        cumulative_followers_of_mentioned_users DESC;
    """
    start_time = time.time()
    cur.execute(query)
    results = cur.fetchall()
    end_time = time.time()
    with open("results/{}.txt".format(table_name), "a") as file:
        file.write(f"Execution Time for OLAP Query 6: {end_time - start_time} s\n")

    return results


if __name__ == "__main__":
    dbname = 'twitter'        
    user = 'postgres'         
    password = 'bdma1234' 

    conn, cur = connect_to_postgres(dbname, user, password)
    results = analyze_twitter_influencers(cur, args.table_name)
    
    for row in results:
        print(row)

    cur.close()
    conn.close()
