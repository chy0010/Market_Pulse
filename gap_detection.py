import json
import sqlite3
import feedparser
import requests
import time
from brand_ticker_map import get_ticker

DB_PATH = "marketpulse.db"
STOCKTWITS_URL = "https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
RSS_FEEDS = [
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US",
    "https://feeds.reuters.com/reuters/businessNews",
]

HEADERS = {"User-Agent": "MarketPulse/1.0"}


def get_rss_mention_count(ticker: str, brand: str) -> int:
    """Count recent RSS headlines mentioning the ticker or brand."""
    count = 0
    search_terms = [ticker.lower(), brand.lower().split()[0]]

    for url_template in RSS_FEEDS:
        url = url_template.format(ticker=ticker)
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:20]:
                title = entry.get("title", "").lower()
                summary = entry.get("summary", "").lower()
                if any(t in title or t in summary for t in search_terms):
                    count += 1
        except Exception:
            pass
        time.sleep(0.3)
    return count


def get_stocktwits_volume(ticker: str) -> int:
    """Get recent StockTwits message count for a ticker."""
    try:
        resp = requests.get(
            STOCKTWITS_URL.format(ticker=ticker),
            headers=HEADERS,
            timeout=8
        )
        if resp.status_code == 200:
            data = resp.json()
            messages = data.get("messages", [])
            return len(messages)
    except Exception:
        pass
    return 0


def get_tier2_youtube_mention_count(ticker: str, brand: str) -> int:
    """Count Tier 2 YouTube posts (institutional media) mentioning the brand or ticker in the last 7 days."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        search_terms = [ticker.lower(), brand.lower().split()[0]]
        cutoff = (
            __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
            - __import__("datetime").timedelta(days=7)
        ).isoformat()

        cursor.execute(
            """SELECT text FROM raw_posts
               WHERE source_tier = 'tier2'
                 AND timestamp >= ?""",
            (cutoff,)
        )
        rows = cursor.fetchall()
        conn.close()

        count = 0
        for (text,) in rows:
            t = text.lower()
            if any(term in t for term in search_terms):
                count += 1
        return count
    except Exception:
        return 0


def calculate_institutional_awareness(ticker: str, brand: str) -> float:
    """Return a 0–100 institutional awareness score."""
    rss_count = get_rss_mention_count(ticker, brand)
    st_count = get_stocktwits_volume(ticker)
    yt_tier2_count = get_tier2_youtube_mention_count(ticker, brand)

    # normalize:
    #   rss_count max ~10  → 40 pts
    #   stocktwits max ~30 → 40 pts
    #   yt_tier2  max ~10  → 20 pts
    rss_score = min(40, rss_count * 4)
    st_score = min(40, st_count * 1.35)
    yt_score = min(20, yt_tier2_count * 2)
    return round(rss_score + st_score + yt_score, 1)


def calculate_gap(consumer_score: float, institutional_score: float) -> dict:
    gap = consumer_score - institutional_score
    if gap > 30:
        status = "BUY_WATCH"
    elif gap > 10:
        status = "MONITOR"
    else:
        status = "NEUTRAL"
    return {"gap_score": round(gap, 1), "status": status}


def run():
    with open("brand_scores.json") as f:
        brand_scores = json.load(f)

    # only process brands with a known ticker and meaningful consumer signal
    candidates = [b for b in brand_scores if b.get("ticker") and b["score"] >= 35]

    # deduplicate tickers — keep highest score per ticker
    seen_tickers = {}
    for b in candidates:
        t = b["ticker"]
        if t not in seen_tickers or b["score"] > seen_tickers[t]["score"]:
            seen_tickers[t] = b
    candidates = list(seen_tickers.values())

    print(f"Running gap detection on {len(candidates)} brands...\n")
    print(f"{'Brand':<28} {'Ticker':<6} {'Consumer':>9} {'Instit.':>8} {'Gap':>6}  Status")
    print("-" * 72)

    results = []
    for b in candidates:
        ticker = b["ticker"]
        brand = b["brand"]
        consumer_score = b["score"]

        inst_score = calculate_institutional_awareness(ticker, brand)
        gap_data = calculate_gap(consumer_score, inst_score)

        row = {
            "brand": brand,
            "ticker": ticker,
            "consumer_score": consumer_score,
            "institutional_score": inst_score,
            **gap_data,
        }
        results.append(row)

        status_icon = {"BUY_WATCH": "🔴", "MONITOR": "🟡", "NEUTRAL": "⚪"}.get(gap_data["status"], "")
        print(f"  {brand:<26} {ticker:<6} {consumer_score:>9.1f} {inst_score:>8.1f} "
              f"{gap_data['gap_score']:>6.1f}  {status_icon} {gap_data['status']}")

    results.sort(key=lambda x: x["gap_score"], reverse=True)

    with open("gap_scores.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved {len(results)} gap scores → gap_scores.json")


if __name__ == "__main__":
    run()
