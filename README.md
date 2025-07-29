# Reddit Sub Analysis

This repository contains utilities for collecting and analysing Reddit content in BigQuery. Posts and comments from a subreddit can be ingested and later categorised into thematic clusters using OpenAI embeddings.

## Contents

| File | Description |
|------|-------------|
| `main.py` | Command line tool that downloads posts and comments from a subreddit and stores them in BigQuery. Tables are created automatically if they do not exist. |
| `classify_posts.py` | Clusters posts by embedding the text of each post with OpenAI, running K‑means, and labelling each cluster via GPT‑4o. The resulting `post_type` labels are written back to BigQuery. |
| `Subreddit_Classification.ipynb` | Notebook demonstrating how to classify posts interactively. |
| `requirements.txt` | Python dependencies needed to run the scripts. |

## Usage

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Ingest recent posts and comments:
   ```bash
   python main.py --project <gcp-project> --subreddit <name>
   ```
3. Classify posts:
   ```bash
   python classify_posts.py --project <gcp-project> --subreddit <name>
   ```
   The script expects `OPENAI_API_KEY` to be available (for example in a `.env` file).


## run with...
+venv\Scripts\python.exe main.py --subreddit Leicester --days 365 --project subreddit-analysis-467210 --dataset subreddit
+
+
+run classify with
+
+venv\Scripts\python.exe classify_posts.py --project subreddit-analysis-467210 --dataset subreddit --subreddit Leicester --lookback_days 365 --k 8