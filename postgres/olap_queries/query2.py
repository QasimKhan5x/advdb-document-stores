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

def analyze_content_evolution(cur, table_name, specific_words):
    """Analyze content evolution over time with user engagement."""
    words_regex = '|'.join(specific_words)  # Create a regex pattern to match any of the specific words
    query = f"""
    WITH word_split AS (
        SELECT
            SUBSTRING(data->>'created_at', 1, 7) AS month,
            UNNEST(string_to_array(data->>'text', ' ')) AS word,
            CASE WHEN data->>'retweet_count' ~ '^[0-9]+$' THEN (data->>'retweet_count')::int ELSE 0 END AS retweet_count,
            CASE WHEN (data->>'favorited')::boolean THEN 1 ELSE 0 END AS favorited,
            data->'user'->>'location' AS user_location,
            CASE WHEN data->'user'->>'followers_count' ~ '^[0-9]+$' THEN (data->'user'->>'followers_count')::int ELSE 0 END AS user_followers_count
        FROM
            {table_name}
        WHERE
            data->>'text' ~* '{words_regex}'  -- Use regular expression for matching
    )
    SELECT
        month,
        word,
        user_location,
        CASE
            WHEN user_followers_count > 10000 THEN 'Influencer'
            ELSE 'Regular User'
        END AS user_type,
        AVG(retweet_count + favorited) AS avg_engagement,
        COUNT(*) AS usage_count
    FROM
        word_split
    WHERE
        word = ANY(ARRAY[{', '.join('%s' for _ in specific_words)}])
    GROUP BY
        month, word, user_location, user_type
    ORDER BY
        month, usage_count DESC, avg_engagement DESC;
    """
    start_time = time.time()
    cur.execute(query, specific_words)
    results = cur.fetchall()
    end_time = time.time()
    with open("results/{}.txt".format(table_name), "a") as file:
        file.write(f"Execution Time for OLAP Query 2: {end_time - start_time} s\n")

    return results

if __name__ == "__main__":
    dbname = 'twitter'        
    user = 'postgres'         
    password = 'bdma1234' 
    specific_words = ["twitter", "love"]  

    conn, cur = connect_to_postgres(dbname, user, password)
    results = analyze_content_evolution(cur, args.table_name, specific_words)
    
    for row in results:
        print(row)

    cur.close()
    conn.close()
