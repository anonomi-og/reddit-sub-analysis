# user_activity.py

import os
import time
from collections import Counter, defaultdict
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery

import praw
from tqdm import tqdm

# Load .env
load_dotenv()

# Reddit API setup
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")

)

# BigQuery setup
bq = bigquery.Client()
PROJECT = "subreddit-analysis-467210"
DATASET = "subreddit"
POSTS_TABLE = f"{PROJECT}.{DATASET}.posts"
TARGET_TABLE = f"{PROJECT}.{DATASET}.user_subreddit_activity"

# Step 1: Get top 25 authors
query = f"""
SELECT author, COUNT(*) AS post_count
FROM `{POSTS_TABLE}`
WHERE author NOT IN ('[deleted]', 'AutoModerator')
GROUP BY author
ORDER BY post_count DESC
LIMIT 25
"""
top_authors_df = bq.query(query).to_dataframe()
authors = top_authors_df["author"].tolist()

# Step 2: Collect user activity
results = []

for author in tqdm(authors, desc="Fetching user activity"):
    try:
        redditor = reddit.redditor(author)

        # Submissions
        sub_counts = Counter(
            submission.subreddit.display_name
            for submission in redditor.submissions.new(limit=150)
            if submission.subreddit is not None
        )
        for subreddit, count in sub_counts.items():
            results.append({
                "author": author,
                "subreddit": subreddit,
                "type": "submission",
                "count": count
            })
        time.sleep(1)

        # Comments
        com_counts = Counter(
            comment.subreddit.display_name
            for comment in redditor.comments.new(limit=150)
            if comment.subreddit is not None
        )
        for subreddit, count in com_counts.items():
            results.append({
                "author": author,
                "subreddit": subreddit,
                "type": "comment",
                "count": count
            })
        time.sleep(1)

    except Exception as e:
        if "RATELIMIT" in str(e).upper() or "rate limit" in str(e).lower():
            print(f"[Rate Limit] Waiting 60s for Reddit to recover: {e}")
            time.sleep(60)
            continue
        print(f"[Error] Skipping {author}: {e}")
        continue




# Step 3: Convert to DataFrame
df = pd.DataFrame(results)

# Step 4: Upload to BigQuery
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    schema=[
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("subreddit", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("count", "INTEGER"),
    ]
)

bq.load_table_from_dataframe(df, TARGET_TABLE, job_config=job_config).result()
print(f"✅ Uploaded {len(df)} rows to {TARGET_TABLE}")
