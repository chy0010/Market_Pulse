import requests
import sqlite3
import time
from datetime import datetime, timezone

DB_PATH = "marketpulse.db"

SUBREDDITS = [
    "BuyItForLife",
    "frugal",
    "SkincareAddiction",
    "malefashionadvice",
    "financialindependence",
    "Coffee",
]

HEADERS = {"User-Agent": "MarketPulse/1.0 (research project)"}


def fetch_subreddit_posts(subreddit: str, limit: int = 100) -> list[dict]:
    url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        posts = response.json()["data"]["children"]
        return [p["data"] for p in posts]
    except Exception as e:
        print(f"  [!] Failed to fetch r/{subreddit}: {e}")
        return []


def save_posts(posts: list[dict], subreddit: str, conn: sqlite3.Connection):
    cursor = conn.cursor()
    saved = 0
    for post in posts:
        text = f"{post.get('title', '')} {post.get('selftext', '')}".strip()
        if not text:
            continue
        ts = datetime.fromtimestamp(post.get("created_utc", 0), tz=timezone.utc).isoformat()
        try:
            cursor.execute(
                """
                INSERT INTO raw_posts (text, source, platform, timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (text, f"r/{subreddit}", "reddit", ts),
            )
            saved += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return saved


def run():
    conn = sqlite3.connect(DB_PATH)
    total = 0
    for subreddit in SUBREDDITS:
        print(f"Fetching r/{subreddit}...")
        posts = fetch_subreddit_posts(subreddit)
        saved = save_posts(posts, subreddit, conn)
        print(f"  → {saved} posts saved")
        total += saved
        time.sleep(1)  # be polite to Reddit's servers
    conn.close()
    print(f"\nDone. {total} total posts saved to {DB_PATH}")


if __name__ == "__main__":
    run()
