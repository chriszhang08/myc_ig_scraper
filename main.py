import json
import sqlite3
import time

from apify import Actor
from apify_client import ApifyClient

usernames = []

# open instagrams.json and get all the usernames
with open('instagrams.json') as f:
    data = json.load(f)
    for account in data:
        if account['instagram'] is not None:
            usernames.append(account['instagram'])

# print(usernames)

# if the database does not exist, create it
conn = sqlite3.connect('instagrams.db')
c = conn.cursor()

# create the table if it does not exist
c.execute('''CREATE TABLE IF NOT EXISTS instagrams
             (username text, posts_last integer, followers_last integer, following_last integer, posts_now integer, 
             followers_now integer, following_now integer, percent_growth float)''')

# initialize all usernames, and set all counts to -1
for username in usernames:
    c.execute("INSERT INTO instagrams VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (username, -1, -1, -1, -1, -1, -1, None))

# Initialize the ApifyClient with your Apify API token
client = ApifyClient("")

# profile the following script
start_time = time.time()

# Prepare the Actor input
run_input = {"usernames": usernames}

# Run the Actor and wait for it to finish
run = client.actor("apify/instagram-profile-scraper").call(run_input=run_input)

current_ig_count = {}
banned_accounts = []

# Fetch and print Actor results from the run's dataset (if there are any)
print("💾 Check your data here: https://console.apify.com/storage/datasets/" + run["defaultDatasetId"])
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    try:
        print(item["username"], item['postsCount'], item['followersCount'], item['followsCount'])
        current_ig_count[item["username"]] = [item['postsCount'], item['followersCount'], item['followsCount']]
    except KeyError:
        print(item)
        banned_accounts.append(item["username"])
    finally:
        pass

end_time = time.time()

print(f"Time taken: {end_time - start_time} seconds")

# merge into sqlite database,
for username in usernames:
    try:
        if username in banned_accounts:
            c.execute(
                "UPDATE instagrams SET posts_last = posts_now, followers_last = followers_now, following_last = following_now, "
                "      posts_now = ?, followers_now = ?, following_now = ? WHERE username = ?",
                (-1, -1, -1, username))
        else:
            c.execute(
                "UPDATE instagrams SET posts_last = posts_now, followers_last = followers_now, following_last = following_now, "
                "      posts_now = ?, followers_now = ?, following_now = ? WHERE username = ?",
                (current_ig_count[username][0], current_ig_count[username][1], current_ig_count[username][2], username))
    except KeyError:
        print(f"KeyError: {username}")
        print(current_ig_count)

# commit the changes
conn.commit()

# select all rows where followers_now - followers_last > 1000
c.execute("SELECT * FROM instagrams WHERE followers_now - followers_last > 1000")
for row in c.fetchall():
    print(row)

c.execute("UPDATE instagrams SET percent_growth = (followers_now - followers_last) / followers_last")

# select from the table where the difference in followers is greater than 10% and num of followers is greater than 1000
c.execute("SELECT * FROM instagrams WHERE percent_growth > 5 AND followers_now > 1000")
for row in c.fetchall():
    print(row)

# export the table to a csv
c.execute("SELECT * FROM instagrams")
rows = c.fetchall()
with open('instagrams.csv', 'w') as f:
    for row in rows:
        f.write(','.join(map(str, row)) + '\n')

# close the connection
conn.close()

# 📚 Want to learn more 📖? Go to → https://docs.apify.com/api/client/python/docs/quick-start
