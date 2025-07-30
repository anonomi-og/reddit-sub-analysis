import os, sys, argparse, time, datetime as dt
from typing import List, Dict

import praw
from google.cloud import bigquery, secretmanager
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def load_env():
    if os.path.exists(".env"):
        load_dotenv(".env")

def get_secret(project_id: str, name: str) -> str:
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{project_id}/secrets/{name}/versions/latest"
        resp = client.access_secret_version(request={"name": secret_path})
        return resp.payload.data.decode()
    except Exception:
        return os.getenv(name)

def utc_ts(d: dt.datetime) -> int:
    return int(d.timestamp())

# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------
def ensure_tables(project: str, dataset: str):
    bq = bigquery.Client(project=project)
    ds_ref = bigquery.DatasetReference(project, dataset)
    try:
        bq.get_dataset(ds_ref)
    except Exception:
        ds = bigquery.Dataset(ds_ref)
        ds.location = "EU"
        bq.create_dataset(ds, exists_ok=True)

    # ------------------------------------------------------------------
    # ✨  SCHEMAS – extra columns appended at the end (all NULLABLE) ✨
    # ------------------------------------------------------------------
    schemas = {
        "posts": [
            # core
            ("id", "STRING", True),
            ("subreddit", "STRING", False),
            ("created_utc", "TIMESTAMP", False),
            ("author", "STRING", False),
            ("title", "STRING", False),
            ("selftext", "STRING", False),
            ("score", "INTEGER", False),
            ("num_comments", "INTEGER", False),
            # extras
            ("upvote_ratio", "FLOAT", False),
            ("total_awards_received", "INTEGER", False),
            ("gilded", "INTEGER", False),
            ("num_crossposts", "INTEGER", False),
            ("over_18", "BOOLEAN", False),
            ("spoiler", "BOOLEAN", False),
            ("stickied", "BOOLEAN", False),
            ("locked", "BOOLEAN", False),
            ("edited", "TIMESTAMP", False),
            ("link_flair_text", "STRING", False),
            ("link_flair_template_id", "STRING", False),
            ("author_flair_text", "STRING", False),
            ("author_flair_template_id", "STRING", False),
            ("is_self", "BOOLEAN", False),
            ("url", "STRING", False),
            ("domain", "STRING", False),
            ("post_hint", "STRING", False),
            ("is_video", "BOOLEAN", False),
        ],
        "comments": [
            # core
            ("id", "STRING", True),
            ("subreddit", "STRING", False),
            ("link_id", "STRING", False),
            ("parent_id", "STRING", False),
            ("created_utc", "TIMESTAMP", False),
            ("author", "STRING", False),
            ("body", "STRING", False),
            ("score", "INTEGER", False),
            # extras
            ("controversiality", "INTEGER", False),
            ("total_awards_received", "INTEGER", False),
            ("gilded", "INTEGER", False),
            ("depth", "INTEGER", False),
            ("edited", "TIMESTAMP", False),
        ],
    }

    refs = {}
    for tbl, fields in schemas.items():
        ref = ds_ref.table(tbl)
        try:
            bq.get_table(ref)
        except Exception:
            schema = [
                bigquery.SchemaField(n, t, mode="REQUIRED" if req else "NULLABLE")
                for n, t, req in fields
            ]
            t = bigquery.Table(ref, schema=schema)
            t.time_partitioning = bigquery.TimePartitioning(
                bigquery.TimePartitioningType.DAY, field="created_utc"
            )
            bq.create_table(t, exists_ok=True)
        refs[tbl] = ref
    return refs["posts"], refs["comments"], bq

# ---------------------------------------------------------------------------
# Reddit helpers
# ---------------------------------------------------------------------------
def init_reddit():
    return praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.getenv("REDDIT_USER_AGENT", "reddit-archive/1.0"),
        check_for_async=False,
    )

def fetch_backwards(reddit, sub, start_ts, end_ts):
    sr = reddit.subreddit(sub)
    while end_ts > start_ts:
        page = sr.search(
            "", sort="new", syntax="lucene", limit=None,
            params={"before": int(end_ts)},
        )
        empty, oldest = True, None
        for post in page:
            empty = False
            if post.created_utc < start_ts:
                return
            yield post
            oldest = post.created_utc if oldest is None else min(oldest, post.created_utc)
        if empty:
            break
        end_ts = int(oldest) - 1
        print(
            f"↘ paging back to "
            f"{dt.datetime.fromtimestamp(end_ts, dt.timezone.utc):%Y‑%m‑%d}"
        )

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    load_env()

    ap = argparse.ArgumentParser()
    ap.add_argument("--subreddit", default=os.getenv("SUBREDDIT", "Leicester"))
    ap.add_argument("--days", type=int, default=int(os.getenv("DAYS", 7)))
    ap.add_argument("--project", default=os.getenv("PROJECT_ID"))
    ap.add_argument("--dataset", default=os.getenv("BQ_DATASET", "subreddit"))
    args = ap.parse_args()

    if not args.project:
        sys.exit("PROJECT_ID missing")

    for key in ["REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET"]:
        if not os.environ.get(key):
            os.environ[key] = get_secret(args.project, key)  # keep original case

    reddit = init_reddit()
    posts_ref, comments_ref, bq = ensure_tables(args.project, args.dataset)

    end_dt = dt.datetime.now(dt.timezone.utc)
    start_dt = end_dt - dt.timedelta(days=args.days)
    start_ts, end_ts = utc_ts(start_dt), utc_ts(end_dt)

    print(f"▶ Pulling r/{args.subreddit} from {start_dt:%Y-%m-%d} to {end_dt:%Y-%m-%d}")

    total_posts = total_comments = 0
    posts_batch: List[Dict] = []
    comments_batch: List[Dict] = []

    for s in fetch_backwards(reddit, args.subreddit, start_ts, end_ts):
        total_posts += 1
        posts_batch.append({
            "id": s.id,
            "subreddit": args.subreddit,
            "created_utc": dt.datetime.fromtimestamp(s.created_utc, dt.timezone.utc).isoformat(),
            "author": getattr(s.author, "name", None),
            "title": s.title,
            "selftext": s.selftext,
            "score": s.score,
            "num_comments": s.num_comments,
            # extras
            "upvote_ratio": s.upvote_ratio,
            "total_awards_received": s.total_awards_received,
            "gilded": s.gilded,
            "num_crossposts": s.num_crossposts,
            "over_18": s.over_18,
            "spoiler": s.spoiler,
            "stickied": s.stickied,
            "locked": s.locked,
            "edited": (
                dt.datetime.fromtimestamp(s.edited, dt.timezone.utc).isoformat()
                if isinstance(s.edited, (int, float)) else None
            ),
            "link_flair_text": s.link_flair_text,
            "link_flair_template_id": s.link_flair_template_id,
            "author_flair_text": s.author_flair_text,
            "author_flair_template_id": s.author_flair_template_id,
            "is_self": s.is_self,
            "url": s.url,
            "domain": s.domain,
            "post_hint": getattr(s, "post_hint", None),
            "is_video": s.is_video,
        })

        s.comments.replace_more(limit=None)
        for c in s.comments.list():
            total_comments += 1
            comments_batch.append({
                "id": c.id,
                "subreddit": args.subreddit,
                "link_id": s.id,
                "parent_id": c.parent_id.split("_")[-1],
                "created_utc": dt.datetime.fromtimestamp(c.created_utc, dt.timezone.utc).isoformat(),
                "author": getattr(c.author, "name", None),
                "body": c.body,
                "score": c.score,
                # extras
                "controversiality": c.controversiality,
                "total_awards_received": c.total_awards_received,
                "gilded": c.gilded,
                "depth": c.depth,
                "edited": (
                    dt.datetime.fromtimestamp(c.edited, dt.timezone.utc).isoformat()
                    if isinstance(c.edited, (int, float)) else None
                ),
            })

        if len(posts_batch) >= 200:
            bq.insert_rows_json(posts_ref, posts_batch)
            posts_batch.clear()
            print(f"  · inserted {total_posts} posts so far…")

        if len(comments_batch) >= 1000:
            bq.insert_rows_json(comments_ref, comments_batch)
            comments_batch.clear()
            print(f"  · inserted {total_comments} comments so far…")

        time.sleep(0.2)

    if posts_batch:
        bq.insert_rows_json(posts_ref, posts_batch)
    if comments_batch:
        bq.insert_rows_json(comments_ref, comments_batch)

    print(f"✅ Done. {total_posts} posts and {total_comments} comments ingested.")

if __name__ == "__main__":
    main()
