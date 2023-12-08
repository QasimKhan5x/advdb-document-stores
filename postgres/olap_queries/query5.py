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

def analyze_user_activity_patterns(cur, table_name):
    """Analyze patterns of users' activity over time, including peak activity hours."""
    query = f"""
    WITH expanded_hashtags AS (
        SELECT
            EXTRACT(HOUR FROM (data->>'created_at')::timestamp) AS hour_of_day,
            data->'user'->>'location' AS user_location,
            jsonb_array_elements(data->'entities'->'hashtags') AS hashtag_element
        FROM
            {table_name}
    )
    SELECT
        hour_of_day,
        user_location,
        hashtag_element->>'text' AS hashtag,
        COUNT(*) AS tweet_count
    FROM
        expanded_hashtags
    GROUP BY
        hour_of_day, user_location, hashtag
    ORDER BY
        hour_of_day, user_location, hashtag;
    """
    start_time = time.time()
    cur.execute(query)
    results = cur.fetchall()
    end_time = time.time()
    with open("results/{}.txt".format(table_name), "a") as file:
        file.write(f"Execution Time for OLAP Query 5: {end_time - start_time} s\n")
    
    return results

if __name__ == "__main__":
    dbname = 'twitter'        
    user = 'postgres'         
    password = 'bdma1234' 

    conn, cur = connect_to_postgres(dbname, user, password)
    results = analyze_user_activity_patterns(cur, args.table_name)
    
    for row in results:
        print(row)

    cur.close()
    conn.close()
