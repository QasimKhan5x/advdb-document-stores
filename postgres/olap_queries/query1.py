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

def determine_influential_users(cur, table_name):
    """Determine influential users based on retweets, favorites, and mentions without using follower count tiers."""
    query = f"""
    SELECT
        data->'user'->>'screen_name' AS screen_name,
        COALESCE(SUM(CASE WHEN data->>'retweet_count' ~ '^[0-9]+$' THEN (data->>'retweet_count')::int ELSE 0 END), 0) AS total_retweets,
        COALESCE(SUM(CASE WHEN (data->>'favorited')::boolean THEN 1 ELSE 0 END), 0) AS favorited_count,
        COUNT(*) AS mention_count,
        COALESCE(SUM(CASE WHEN data->>'retweet_count' ~ '^[0-9]+$' THEN (data->>'retweet_count')::int ELSE 0 END), 0) + COALESCE(SUM(CASE WHEN (data->>'favorited')::boolean THEN 1 ELSE 0 END), 0) AS engagement_score
    FROM
        {table_name},
        LATERAL jsonb_array_elements(data->'entities'->'user_mentions') AS mentions
    GROUP BY
        screen_name
    ORDER BY
        engagement_score DESC,
        mention_count DESC;
    """

    start_time = time.time()
    cur.execute(query)
    end_time = time.time()
    with open("results/{}.txt".format(table_name), "a") as file:
        file.write(f"Execution Time for OLAP Query 1: {end_time - start_time} s\n")

    return cur.fetchall()


if __name__ == "__main__":
    dbname = 'twitter'        
    user = 'postgres'         
    password = 'bdma1234'     
    table_name = args.table_name

    conn, cur = connect_to_postgres(dbname, user, password)
    results = determine_influential_users(cur, table_name)
    
    for row in results:
        print(row)

    cur.close()
    conn.close()
