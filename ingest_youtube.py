import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()

DB_PATH = "marketpulse.db"
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

MAX_COMMENTS_PER_VIDEO = 30

TIER1_CHANNELS = [
    "GoodWorkMB", "morning-brew", "techteller927", "mkbhd", "guistetelle",
    "alexlorenlee", "miso-tech", "firooza", "AaravNarula", "VarunMayya",
    "nikhil.kamath", "TomBilyeu", "DwarkeshPatel", "JeffSu", "iqjayfeng",
    "mreflow", "AaronJack", "harkirat1", "IvyFung", "martinzeman89",
    "RuntimeBRT", "principlesbyraydalio", "BusinessBasicsYT",
    "SimplilearnOfficial", "IBMTechnology", "Vox", "ALifeEngineered",
    "FactasticFeed", "worldaffairsEng", "USAFacts_Official", "LeisRealTalk",
    "robmulla", "NewMachina", "TLDRbusiness",
]

TIER2_CHANNELS = [
    "CNBC", "CNBCi", "CNBCtelevision", "WSJNews", "markets",
    "Bloomberg-News", "business", "BloombergNewEconomy", "FoxBusiness",
    "FinancialTimes", "YahooFinance", "BusinessToday", "Groww", "moneycontrol",
]


def get_youtube_client():
    if not YOUTUBE_API_KEY:
        raise ValueError("YOUTUBE_API_KEY not set in .env")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


def resolve_channel_id(youtube, handle: str) -> str | None:
    """Resolve a channel handle/username to a channel ID."""
    try:
        resp = youtube.channels().list(
            part="id",
            forHandle=handle,
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["id"]
    except Exception:
        pass

    # Fallback: search by channel name
    try:
        resp = youtube.search().list(
            q=handle,
            part="id",
            type="channel",
            maxResults=1,
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["id"]["channelId"]
    except Exception as e:
        print(f"  [!] Could not resolve channel '{handle}': {e}")
    return None


def get_uploads_playlist_id(youtube, channel_id: str) -> str | None:
    """Return the uploads playlist ID for a channel."""
    try:
        resp = youtube.channels().list(
            part="contentDetails",
            id=channel_id,
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except Exception as e:
        print(f"  [!] Could not get uploads playlist for {channel_id}: {e}")
    return None


def get_recent_video_ids(youtube, playlist_id: str, cutoff: datetime) -> list[str]:
    """Return video IDs from the uploads playlist published after cutoff."""
    video_ids = []
    page_token = None

    while True:
        try:
            kwargs = dict(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
            )
            if page_token:
                kwargs["pageToken"] = page_token

            resp = youtube.playlistItems().list(**kwargs).execute()
            for item in resp.get("items", []):
                published = item["snippet"].get("publishedAt", "")
                if published:
                    pub_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                    if pub_dt >= cutoff:
                        vid_id = item["snippet"]["resourceId"]["videoId"]
                        video_ids.append(vid_id)
                    else:
                        return video_ids  # playlist is ordered newest-first

            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        except Exception as e:
            print(f"  [!] Playlist fetch failed: {e}")
            break

    return video_ids


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
        if "disabled" not in str(e).lower():
            print(f"  [!] Comments failed for video {video_id}: {e}")
        return []


def is_video_processed(cursor: sqlite3.Cursor, video_id: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM raw_posts WHERE source = ? LIMIT 1",
        (f"youtube:{video_id}",)
    )
    return cursor.fetchone() is not None


def save_comments(
    comments: list[dict],
    source_tier: str,
    conn: sqlite3.Connection,
) -> int:
    cursor = conn.cursor()
    saved = 0
    for c in comments:
        if not c["text"].strip():
            continue
        try:
            cursor.execute(
                """INSERT INTO raw_posts (text, source, platform, timestamp, source_tier)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    c["text"][:2000],
                    f"youtube:{c['video_id']}",
                    "youtube",
                    c.get("published_at", datetime.now(timezone.utc).isoformat()),
                    source_tier,
                ),
            )
            saved += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return saved


def process_channels(
    youtube,
    conn: sqlite3.Connection,
    handles: list[str],
    source_tier: str,
    cutoff: datetime,
) -> int:
    cursor = conn.cursor()
    total_saved = 0

    for handle in handles:
        print(f"  Channel: @{handle} [{source_tier}]")

        channel_id = resolve_channel_id(youtube, handle)
        if not channel_id:
            print(f"    → skipped (could not resolve)")
            continue

        playlist_id = get_uploads_playlist_id(youtube, channel_id)
        if not playlist_id:
            print(f"    → skipped (no uploads playlist)")
            continue

        video_ids = get_recent_video_ids(youtube, playlist_id, cutoff)
        print(f"    → {len(video_ids)} recent video(s)")

        channel_saved = 0
        for vid_id in video_ids:
            if is_video_processed(cursor, vid_id):
                print(f"    → video {vid_id} already processed, skipping")
                continue

            comments = fetch_comments(youtube, vid_id)
            saved = save_comments(comments, source_tier, conn)
            channel_saved += saved
            time.sleep(0.2)

        print(f"    → {channel_saved} comments saved")
        total_saved += channel_saved
        time.sleep(0.5)

    return total_saved


def run():
    youtube = get_youtube_client()
    conn = sqlite3.connect(DB_PATH)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    print(f"Ingesting YouTube channels (last 24h, cutoff={cutoff.isoformat()})\n")

    print("=== Tier 1: Consumer Signal Channels ===")
    t1_saved = process_channels(youtube, conn, TIER1_CHANNELS, "tier1", cutoff)

    print("\n=== Tier 2: Institutional Media Channels ===")
    t2_saved = process_channels(youtube, conn, TIER2_CHANNELS, "tier2", cutoff)

    conn.close()
    print(f"\nDone. tier1={t1_saved} comments, tier2={t2_saved} comments saved to {DB_PATH}")


if __name__ == "__main__":
    run()
