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

def update_user_profiles_based_on_activity(cur, table, tweet_threshold):
    """ Update user profiles who have tweeted more than the threshold. """
    update_query = f"""
    UPDATE {table}
    SET data = jsonb_set(data, '{{user,description}}', 
                         to_jsonb(data->'user'->>'description' || ' Frequent Tweeter'))
    FROM (
        SELECT data->'user'->>'id_str' as user_id
        FROM {table}
        GROUP BY user_id
        HAVING COUNT(*) > {tweet_threshold}
    ) AS subquery
    WHERE data->'user'->>'id_str' = subquery.user_id;
    """
    cur.execute(update_query)

def tag_users_based_on_topic_interaction(cur, table, keyword):
    """ Tag users based on interaction with specific keywords or hashtags. """
    update_query = f"""
    UPDATE {table}
    SET data = jsonb_set(data, '{{user,description}}', 
                         to_jsonb(data->'user'->>'description' || ' Interested in {keyword}'))
    WHERE data->>'text' LIKE '%{keyword}%' OR 
          data->'entities'->'hashtags' @> '[{{"text": "{keyword}"}}]'::jsonb;
    """
    cur.execute(update_query)

# Updating user profiles based on activity
start_time = time.time()
update_user_profiles_based_on_activity(cur, table_name, 1)
end_time = time.time()
with open("results/{}.txt".format(table_name), "a") as file:
    file.write(f"Execution Time for OLTP Query 5: {end_time - start_time} s\n")

# Tagging users based on interaction with 'life'
start_time = time.time()
tag_users_based_on_topic_interaction(cur, table_name, 'life')
end_time = time.time()
with open("results/{}.txt".format(table_name), "a") as file:
    file.write(f"Execution Time for OLTP Query 6: {end_time - start_time} s\n")

conn.commit()
cur.close()
conn.close()

