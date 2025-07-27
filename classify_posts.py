# classify_posts.py – one‑shot post‑type discovery + labelling for subreddit posts
# ---------------------------------------------------------------------------
"""
Adds a **post_type** column to the BigQuery *posts* table:
  1. Download <lookback_days> of posts (title + self‑text) from BigQuery
  2. Embed texts with OpenAI (text‑embedding‑3‑small)
  3. Cluster embeddings with K‑means (k clusters)
  4. Ask GPT‑4o‑mini to name each cluster (2‑4 words)
  5. Write labels back into BigQuery using an UPDATE … FROM join

Example (Windows PowerShell):

    venv\Scripts\python.exe classify_posts.py \
        --project subreddit-analysis-467210 \
        --dataset subreddit \
        --subreddit Leicester \
        --lookback_days 365 \
        --k 8

The script autoloads **OPENAI_API_KEY** from `.env` via python‑dotenv, so
no need to export the key manually while testing.
"""

import os
import argparse
import json
from typing import List, Dict

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from openai import OpenAI
from google.cloud import bigquery
from tqdm.auto import tqdm
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

load_dotenv(".env")  # loads OPENAI_API_KEY if present

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def fetch_posts(bq: bigquery.Client, project: str, dataset: str, days: int, subreddit: str) -> pd.DataFrame:
    """Pull <days> of posts from BigQuery and return as DataFrame."""
    sql = f"""
        SELECT id,
               CONCAT(COALESCE(title, ''), ' ', COALESCE(selftext, '')) AS text
        FROM `{project}.{dataset}.posts`
        WHERE subreddit = @sub AND created_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @d DAY)
    """
    job = bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("sub", "STRING", subreddit),
                bigquery.ScalarQueryParameter("d", "INT64", days),
            ]
        ),
    )
    return job.to_dataframe()


def embed_texts(client: OpenAI, texts: List[str], *, model: str = "text-embedding-3-small", batch: int = 100) -> np.ndarray:
    """Embed texts using OpenAI in batches; return ndarray (n, dims)."""
    out = []
    for i in tqdm(range(0, len(texts), batch), desc="Embedding"):
        chunk = texts[i : i + batch]
        resp = client.embeddings.create(model=model, input=chunk)
        out.extend([e.embedding for e in resp.data])
    return np.asarray(out, dtype=np.float32)


def label_cluster(client: OpenAI, sample_posts: List[str]) -> str:
    """Ask GPT‑4o‑mini to give a short name for a cluster."""
    prompt = (
        "You are summarising Reddit post topics. "
        "Here are example posts from one cluster:\n\n" +
        "\n\n---\n\n".join(sample_posts) +
        "\n\nProvide a VERY short (2‑4 words) name for the overall topic:"
    )
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
    )
    return resp.choices[0].message.content.strip()


def merge_labels(bq: bigquery.Client, df_labels: pd.DataFrame, project: str, dataset: str):
    """Load id/post_type pairs into temp table then UPDATE posts."""
    tmp = f"{dataset}.post_labels_tmp"

    job_cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("post_type", "STRING"),
        ],
    )
    bq.load_table_from_dataframe(df_labels[["id", "post_type"]], tmp, job_config=job_cfg).result()

    sql = f"""
    UPDATE `{project}.{dataset}.posts` AS T
    SET post_type = S.post_type
    FROM `{project}.{tmp}` AS S
    WHERE T.id = S.id
    """
    bq.query(sql).result()
    bq.delete_table(tmp, not_found_ok=True)

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Cluster + label subreddit posts")
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", default="subreddit")
    parser.add_argument("--subreddit", default="Leicester")
    parser.add_argument("--lookback_days", type=int, default=365)
    parser.add_argument("--k", type=int, default=8, help="number of clusters")
    args = parser.parse_args()

    if not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is missing – add it to .env or export it")

    # 1. Fetch posts
    bq = bigquery.Client(project=args.project)
    df = fetch_posts(bq, args.project, args.dataset, args.lookback_days, args.subreddit)
    if df.empty:
        print("No posts found – nothing to classify.")
        return
    print(f"Fetched {len(df)} posts from BigQuery…")

    # 2. Embed
    client = OpenAI()
    embeds = embed_texts(client, df["text"].tolist())

    # 3. Cluster
    print("Clustering with K‑means…")
    km = KMeans(n_clusters=args.k, n_init="auto", random_state=42).fit(embeds)
    df["cluster_id"] = km.labels_

    # 4. Label clusters
    print("Labelling clusters with GPT‑4o…")
    labels: Dict[str, str] = {}
    for cid in sorted(df["cluster_id"].unique()):
        cluster_posts = df[df.cluster_id == cid]["text"]
        sample = cluster_posts.sample(min(5, len(cluster_posts)), random_state=cid).tolist()
        labels[str(int(cid))] = label_cluster(client, sample)
    df["post_type"] = df["cluster_id"].map(lambda i: labels[str(int(i))])

    print("Cluster labels:")
    print(json.dumps(labels, indent=2, ensure_ascii=False))

    # 5. Merge back to BigQuery
    print("Merging labels into BigQuery…")
    merge_labels(bq, df, args.project, args.dataset)
    print("✅ Classification complete.")


if __name__ == "__main__":
    main()
