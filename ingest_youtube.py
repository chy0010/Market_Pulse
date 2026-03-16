import os
import sqlite3
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()

DB_PATH = "marketpulse.db"
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# Search queries that surface obsession/spend-confession/discovery language
SEARCH_QUERIES = [
    "honest product review worth it 2026",
    "best skincare routine obsessed",
    "I switched to this product",
    "coffee gear setup espresso",
    "best budget buy it for life",
    "unboxing reaction amazing",
    "hidden gem product recommendation",
]

MAX_RESULTS_PER_QUERY = 10   # videos per query
MAX_COMMENTS_PER_VIDEO = 30  # comments per video


def get_youtube_client():
    if not YOUTUBE_API_KEY:
        raise ValueError("YOUTUBE_API_KEY not set in .env")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


def search_videos(youtube, query: str) -> list[str]:
    """Return list of video IDs for a search query."""
    try:
        resp = youtube.search().list(
            q=query,
            part="id",
            type="video",
            maxResults=MAX_RESULTS_PER_QUERY,
            relevanceLanguage="en",
            order="relevance",
        ).execute()
        return [item["id"]["videoId"] for item in resp.get("items", [])]
    except Exception as e:
        print(f"  [!] Search failed for '{query}': {e}")
        return []


def fetch_comments(youtube, video_id: str) -> list[dict]:
    """Fetch top-level comments for a video."""
    try:
        resp = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=MAX_COMMENTS_PER_VIDEO,
            order="relevance",
            textFormat="plainText",
        ).execute()

        comments = []
        for item in resp.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "text": snippet.get("textDisplay", ""),
                "published_at": snippet.get("publishedAt", ""),
                "video_id": video_id,
            })
        return comments
    except Exception as e:
        # disabled comments or quota exceeded
        if "disabled" not in str(e).lower():
            print(f"  [!] Comments failed for video {video_id}: {e}")
        return []


def save_comments(comments: list[dict], query: str, conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    saved = 0
    for c in comments:
        if not c["text"].strip():
            continue
        try:
            cursor.execute(
                "INSERT INTO raw_posts (text, source, platform, timestamp) VALUES (?, ?, ?, ?)",
                (
                    c["text"][:2000],
                    f"youtube:{c['video_id']}",
                    "youtube",
                    c.get("published_at", datetime.now(timezone.utc).isoformat()),
                ),
            )
            saved += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return saved


def run():
    youtube = get_youtube_client()
    conn = sqlite3.connect(DB_PATH)
    total_saved = 0

    for query in SEARCH_QUERIES:
        print(f"Searching: \"{query}\"")
        video_ids = search_videos(youtube, query)
        print(f"  → {len(video_ids)} videos found")

        query_saved = 0
        for vid_id in video_ids:
            comments = fetch_comments(youtube, vid_id)
            saved = save_comments(comments, query, conn)
            query_saved += saved
            time.sleep(0.2)

        print(f"  → {query_saved} comments saved")
        total_saved += query_saved
        time.sleep(1)

    conn.close()
    print(f"\nDone. {total_saved} YouTube comments saved to {DB_PATH}")


if __name__ == "__main__":
    run()
