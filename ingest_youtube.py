import os
import json
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()

DB_PATH = "marketpulse.db"
CHANNEL_CACHE_FILE = "channel_cache.json"
PROCESSED_VIDEOS_FILE = "processed_videos.json"

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

TIER1_MAX_COMMENTS = 40
TIER2_MAX_COMMENTS = 20
MAX_VIDEOS_PER_CHANNEL = 2

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


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def load_json_file(path: str, default) -> dict | list:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json_file(path: str, data) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# YouTube client
# ---------------------------------------------------------------------------

def get_youtube_client():
    if not YOUTUBE_API_KEY:
        raise ValueError("YOUTUBE_API_KEY not set in .env")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


# ---------------------------------------------------------------------------
# Channel resolution (with cache)
# ---------------------------------------------------------------------------

def resolve_channel(youtube, handle: str, cache: dict) -> tuple[str | None, str | None]:
    """Return (channel_id, uploads_playlist_id) for a handle, using cache."""
    if handle in cache:
        entry = cache[handle]
        return entry.get("channel_id"), entry.get("playlist_id")

    channel_id = None

    # Primary: forHandle lookup
    try:
        resp = youtube.channels().list(
            part="id,contentDetails",
            forHandle=handle,
        ).execute()
        items = resp.get("items", [])
        if items:
            channel_id = items[0]["id"]
            playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
            cache[handle] = {"channel_id": channel_id, "playlist_id": playlist_id}
            return channel_id, playlist_id
    except Exception:
        pass

    # Fallback: search by name then fetch contentDetails
    try:
        resp = youtube.search().list(
            q=handle, part="id", type="channel", maxResults=1,
        ).execute()
        items = resp.get("items", [])
        if items:
            channel_id = items[0]["id"]["channelId"]
    except Exception as e:
        print(f"  [!] Could not resolve channel '{handle}': {e}")
        return None, None

    if not channel_id:
        return None, None

    try:
        resp = youtube.channels().list(
            part="contentDetails", id=channel_id,
        ).execute()
        items = resp.get("items", [])
        if items:
            playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
            cache[handle] = {"channel_id": channel_id, "playlist_id": playlist_id}
            return channel_id, playlist_id
    except Exception as e:
        print(f"  [!] Could not get uploads playlist for '{handle}': {e}")

    return None, None


# ---------------------------------------------------------------------------
# Video discovery
# ---------------------------------------------------------------------------

def get_recent_videos(
    youtube, playlist_id: str, cutoff: datetime
) -> list[dict]:
    """Return [{video_id, title}] for videos published after cutoff."""
    videos = []
    page_token = None

    while True:
        try:
            kwargs = dict(part="snippet", playlistId=playlist_id, maxResults=50)
            if page_token:
                kwargs["pageToken"] = page_token

            resp = youtube.playlistItems().list(**kwargs).execute()
            for item in resp.get("items", []):
                snippet = item["snippet"]
                published = snippet.get("publishedAt", "")
                if published:
                    pub_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                    if pub_dt >= cutoff:
                        videos.append({
                            "video_id": snippet["resourceId"]["videoId"],
                            "title": snippet.get("title", ""),
                        })
                    else:
                        return videos  # playlist is newest-first; stop early

            page_token = resp.get("nextPageToken")
            if not page_token:
                break
            time.sleep(0.2)
        except Exception as e:
            print(f"  [!] Playlist fetch failed: {e}")
            break

    return videos


# ---------------------------------------------------------------------------
# Comment fetching
# ---------------------------------------------------------------------------

def fetch_comments(youtube, video_id: str, max_comments: int) -> list[dict]:
    """Fetch top-level comments for a video."""
    try:
        resp = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_comments,
            order="relevance",
            textFormat="plainText",
        ).execute()

        comments = []
        for item in resp.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "text": snippet.get("textDisplay", ""),
                "published_at": snippet.get("publishedAt", ""),
            })
        return comments
    except Exception as e:
        if "disabled" not in str(e).lower():
            print(f"  [!] Comments failed for video {video_id}: {e}")
        return []


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def save_comments(
    conn: sqlite3.Connection,
    comments: list[dict],
    video_id: str,
    video_title: str,
    source_tier: str,
) -> int:
    cursor = conn.cursor()
    saved = 0
    for c in comments:
        if not c["text"].strip():
            continue
        try:
            cursor.execute(
                """INSERT INTO raw_posts
                       (text, source, platform, timestamp, source_tier, video_title)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    c["text"][:2000],
                    f"youtube:{video_id}",
                    "youtube",
                    c.get("published_at", datetime.now(timezone.utc).isoformat()),
                    source_tier,
                    video_title[:500] if video_title else None,
                ),
            )
            saved += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    return saved


# ---------------------------------------------------------------------------
# Channel processing
# ---------------------------------------------------------------------------

def process_channels(
    youtube,
    conn: sqlite3.Connection,
    handles: list[str],
    source_tier: str,
    cutoff: datetime,
    channel_cache: dict,
    processed_videos: set,
    max_comments: int,
) -> int:
    total_saved = 0

    for handle in handles:
        print(f"  @{handle} [{source_tier}]")

        channel_id, playlist_id = resolve_channel(youtube, handle, channel_cache)
        if not channel_id or not playlist_id:
            print(f"    → skipped (could not resolve)")
            time.sleep(0.2)
            continue

        videos = get_recent_videos(youtube, playlist_id, cutoff)
        new_videos = [v for v in videos if v["video_id"] not in processed_videos][:MAX_VIDEOS_PER_CHANNEL]

        if not videos:
            print(f"    → no new videos in last 24h")
            time.sleep(0.2)
            continue

        print(f"    → {len(videos)} recent video(s), {len(new_videos)} unprocessed")

        channel_saved = 0
        for v in new_videos:
            vid_id = v["video_id"]
            title = v["title"]

            comments = fetch_comments(youtube, vid_id, max_comments)
            saved = save_comments(conn, comments, vid_id, title, source_tier)
            channel_saved += saved
            processed_videos.add(vid_id)
            time.sleep(0.2)

        print(f"    → {channel_saved} comments saved")
        total_saved += channel_saved
        time.sleep(0.2)

    return total_saved


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run():
    youtube = get_youtube_client()
    conn = sqlite3.connect(DB_PATH)
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    cutoff = datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=timezone.utc)  # midnight UTC yesterday

    channel_cache: dict = load_json_file(CHANNEL_CACHE_FILE, {})
    processed_videos: set = set(load_json_file(PROCESSED_VIDEOS_FILE, []))

    print(f"Ingesting YouTube channels (last 24h, cutoff={cutoff.isoformat()})")
    print(f"  Loaded {len(channel_cache)} cached channel entries, "
          f"{len(processed_videos)} processed video IDs\n")

    print("=== Tier 1: Consumer Signal Channels ===")
    t1_saved = process_channels(
        youtube, conn, TIER1_CHANNELS, "tier1", cutoff,
        channel_cache, processed_videos, TIER1_MAX_COMMENTS,
    )

    print("\n=== Tier 2: Institutional Media Channels ===")
    t2_saved = process_channels(
        youtube, conn, TIER2_CHANNELS, "tier2", cutoff,
        channel_cache, processed_videos, TIER2_MAX_COMMENTS,
    )

    conn.close()

    # Persist cache and processed video list
    save_json_file(CHANNEL_CACHE_FILE, channel_cache)
    save_json_file(PROCESSED_VIDEOS_FILE, list(processed_videos))

    print(f"\nDone. tier1={t1_saved}, tier2={t2_saved} comments saved to {DB_PATH}")
    print(f"Cache updated: {len(channel_cache)} channels, "
          f"{len(processed_videos)} processed videos")


if __name__ == "__main__":
    run()
