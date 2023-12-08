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

def analyze_user_behavior_and_content_strategy(cur, table_name):
    """Analyze user behavior and content strategy optimization."""
    query = f"""
    SELECT
        hashtag->>'text' AS hashtag,
        COUNT(*) AS total_tweets,
        SUM(CASE WHEN data->>'retweet_count' ~ '^[0-9]+$' THEN (data->>'retweet_count')::int ELSE 0 END) AS total_retweets,
        SUM(CASE WHEN (data->>'favorited')::boolean THEN 1 ELSE 0 END) AS total_favorites
    FROM
        {table_name},
        LATERAL jsonb_array_elements(data->'entities'->'hashtags') AS hashtag
    GROUP BY
        hashtag
    """
    start_time = time.time()
    cur.execute(query)
    results = cur.fetchall()
    end_time = time.time()
    with open("results/{}.txt".format(table_name), "a") as file:
        file.write(f"Execution Time for OLAP Query 4: {end_time - start_time} s\n")

    return results

if __name__ == "__main__":
    dbname = 'twitter'        
    user = 'postgres'         
    password = 'bdma1234' 

    conn, cur = connect_to_postgres(dbname, user, password)
    results = analyze_user_behavior_and_content_strategy(cur, args.table_name)
    
    for row in results:
        print(row)

    cur.close()
    conn.close()
