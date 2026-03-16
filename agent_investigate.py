"""
Trend Investigation Agent — Phase 2
Triggered when a brand crosses the momentum threshold.
Autonomously pulls more Reddit posts, checks news headlines,
and returns a one-page brief on the brand.
"""
import os
import json
import requests
import feedparser
import anthropic
from dotenv import load_dotenv
from brand_ticker_map import get_ticker

load_dotenv()

HEADERS = {"User-Agent": "MarketPulse/1.0"}
REDDIT_SEARCH = "https://www.reddit.com/search.json?q={query}&sort=relevance&limit=10&t=month"
RSS_SEARCH    = "https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"


def fetch_reddit_posts(brand: str) -> list[str]:
    query = brand.split()[0]  # first word is usually most specific
    try:
        resp = requests.get(REDDIT_SEARCH.format(query=query), headers=HEADERS, timeout=10)
        posts = resp.json()["data"]["children"]
        return [
            f"{p['data'].get('title','')} {p['data'].get('selftext','')}".strip()[:300]
            for p in posts
            if p["data"].get("title")
        ]
    except Exception:
        return []


def fetch_news_headlines(ticker: str) -> list[str]:
    if not ticker:
        return []
    try:
        feed = feedparser.parse(RSS_SEARCH.format(ticker=ticker))
        return [e.get("title", "") for e in feed.entries[:8]]
    except Exception:
        return []


def build_brief(client: anthropic.Anthropic, brand: str, ticker: str | None,
                score_data: dict, reddit_posts: list[str], headlines: list[str]) -> str:

    reddit_block = "\n".join(f"- {p}" for p in reddit_posts[:8]) or "No additional posts found."
    news_block   = "\n".join(f"- {h}" for h in headlines[:6]) or "No recent headlines."

    prompt = f"""You are a consumer intelligence analyst preparing a one-page brief for a financial research team.

Brand: {brand}
Ticker: {ticker or "private / unmapped"}
Momentum Score: {score_data['score']}/100
Signal Type: {score_data.get('dominant_signal_type')}
Signal Count: {score_data['signal_count']} posts
Top Trigger Phrase: "{score_data.get('top_trigger', 'N/A')}"
Breakout Detected: {"YES" if score_data.get('breakout') else "No"}

Recent Reddit posts mentioning this brand:
{reddit_block}

Recent institutional news headlines:
{news_block}

Write a concise brief (max 200 words) covering:
1. What consumers are saying and why it matters
2. Whether institutional coverage has caught up yet
3. The market implication (one clear sentence)
4. Risk factors to watch

Be direct. No fluff."""

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()


def investigate(brand: str, score_data: dict) -> dict:
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    ticker = score_data.get("ticker") or get_ticker(brand)

    print(f"\nInvestigating: {brand} (score={score_data['score']})")
    print("  Fetching Reddit posts...")
    reddit_posts = fetch_reddit_posts(brand)

    print("  Fetching news headlines...")
    headlines = fetch_news_headlines(ticker)

    print("  Generating brief...")
    brief = build_brief(client, brand, ticker, score_data, reddit_posts, headlines)

    result = {
        "brand": brand,
        "ticker": ticker,
        "score": score_data["score"],
        "breakout": score_data.get("breakout", False),
        "reddit_sample_count": len(reddit_posts),
        "headline_count": len(headlines),
        "brief": brief,
    }
    return result


def run(threshold: float = 40.0):
    try:
        with open("brand_scores.json") as f:
            brand_scores = json.load(f)
    except FileNotFoundError:
        print("Run score_brands.py first.")
        return

    # deduplicate by first word of brand name, keep highest score
    seen, candidates = {}, []
    for b in brand_scores:
        key = b["brand"].lower().split()[0]
        if key not in seen and b["score"] >= threshold:
            seen[key] = True
            candidates.append(b)

    if not candidates:
        print(f"No brands above threshold {threshold}.")
        return

    print(f"Found {len(candidates)} brands above threshold {threshold}. Generating briefs...\n")
    briefs = []
    for b in candidates[:5]:  # cap at 5 per run to control cost
        result = investigate(b["brand"], b)
        briefs.append(result)
        print(f"\n{'='*60}")
        print(f"  {result['brand']}  |  {result['ticker'] or 'private'}  |  Score {result['score']}")
        if result["breakout"]:
            print("  *** BREAKOUT DETECTED ***")
        print(f"{'='*60}")
        print(result["brief"])

    with open("investigation_briefs.json", "w") as f:
        json.dump(briefs, f, indent=2)
    print(f"\n\nSaved {len(briefs)} briefs → investigation_briefs.json")


if __name__ == "__main__":
    run()
