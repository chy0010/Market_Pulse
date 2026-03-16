"""
Validation Agent — Phase 2
Triggered on high gap score. Cross-references yfinance price data,
recent analyst upgrades via RSS, and StockTwits sentiment trend.
Returns a validation score: is the consumer signal real and unpriced?
"""
import os
import json
import requests
import feedparser
import yfinance as yf
import anthropic
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

STOCKTWITS_URL = "https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
HEADERS = {"User-Agent": "MarketPulse/1.0"}


def get_price_trend(ticker: str) -> dict:
    try:
        df = yf.download(ticker, period="30d", progress=False, auto_adjust=True)
        close = df["Close"]
        if hasattr(close, "columns"):
            close = close.iloc[:, 0]
        prices = close.dropna()
        if len(prices) < 2:
            return {}
        start, end = float(prices.iloc[0]), float(prices.iloc[-1])
        # recent momentum: last 5 days vs prior 5 days
        recent   = float(prices.iloc[-5:].mean())
        prior    = float(prices.iloc[-10:-5].mean())
        momentum = round((recent - prior) / prior * 100, 2)
        return {
            "30d_return": round((end - start) / start * 100, 2),
            "5d_momentum": momentum,
            "current_price": round(end, 2),
        }
    except Exception:
        return {}


def get_stocktwits_sentiment(ticker: str) -> dict:
    try:
        resp = requests.get(STOCKTWITS_URL.format(ticker=ticker),
                            headers=HEADERS, timeout=8)
        if resp.status_code != 200:
            return {}
        messages = resp.json().get("messages", [])
        bullish = sum(1 for m in messages if m.get("entities", {}).get("sentiment", {}).get("basic") == "Bullish")
        bearish = sum(1 for m in messages if m.get("entities", {}).get("sentiment", {}).get("basic") == "Bearish")
        total = bullish + bearish
        return {
            "message_count": len(messages),
            "bullish": bullish,
            "bearish": bearish,
            "bull_ratio": round(bullish / total, 2) if total else 0.5,
        }
    except Exception:
        return {}


def get_analyst_headlines(ticker: str) -> list[str]:
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        feed = feedparser.parse(url)
        upgrade_keywords = ["upgrade", "raise", "outperform", "buy", "overweight", "price target"]
        return [
            e.get("title", "")
            for e in feed.entries[:20]
            if any(kw in e.get("title", "").lower() for kw in upgrade_keywords)
        ][:5]
    except Exception:
        return []


def validate(client: anthropic.Anthropic, gap_data: dict) -> dict:
    ticker  = gap_data["ticker"]
    brand   = gap_data["brand"]
    gap     = gap_data["gap_score"]
    status  = gap_data["status"]

    price_data   = get_price_trend(ticker)
    st_sentiment = get_stocktwits_sentiment(ticker)
    analyst_news = get_analyst_headlines(ticker)

    prompt = f"""You are a validation analyst for a consumer intelligence system.

A consumer signal has been flagged:
- Brand: {brand} ({ticker})
- Gap Score: {gap} ({status}) — consumer excitement significantly outpaces institutional awareness
- Consumer Momentum Score: {gap_data['consumer_score']}/100
- Institutional Awareness Score: {gap_data['institutional_score']}/100

Market data:
- 30-day price return: {price_data.get('30d_return', 'N/A')}%
- 5-day price momentum: {price_data.get('5d_momentum', 'N/A')}%
- Current price: ${price_data.get('current_price', 'N/A')}

Retail sentiment (StockTwits):
- Messages: {st_sentiment.get('message_count', 'N/A')}
- Bull ratio: {st_sentiment.get('bull_ratio', 'N/A')}

Recent analyst activity:
{chr(10).join(f'- {h}' for h in analyst_news) if analyst_news else '- No recent upgrades/price target changes found'}

Return a JSON object with exactly these fields:
{{
  "validation_score": <integer 0-100, how confident you are the consumer signal is real and not yet priced in>,
  "signal_quality": "strong" | "moderate" | "weak",
  "price_already_moved": true | false,
  "analyst_already_aware": true | false,
  "recommended_action": "WATCH" | "RESEARCH_FURTHER" | "PASS",
  "reason": "<one sentence explanation>"
}}

Return ONLY valid JSON."""

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```", 2)[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    try:
        verdict = json.loads(raw)
    except Exception:
        verdict = {"error": "parse failed", "raw": raw}

    return {
        "brand": brand,
        "ticker": ticker,
        "gap_score": gap,
        "status": status,
        "price_data": price_data,
        "stocktwits": st_sentiment,
        "analyst_upgrades_found": len(analyst_news),
        **verdict,
    }


def run():
    try:
        with open("gap_scores.json") as f:
            gap_scores = json.load(f)
    except FileNotFoundError:
        print("Run gap_detection.py first.")
        return

    candidates = [g for g in gap_scores if g["status"] in ("BUY_WATCH", "MONITOR")]
    if not candidates:
        print("No BUY_WATCH or MONITOR signals to validate.")
        return

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    print(f"Validating {len(candidates)} gap signals...\n")

    results = []
    for g in candidates:
        print(f"  Validating {g['brand']} ({g['ticker']})...")
        result = validate(client, g)
        results.append(result)

        action_colors = {"WATCH": "🟢", "RESEARCH_FURTHER": "🟡", "PASS": "⚪"}
        icon = action_colors.get(result.get("recommended_action", ""), "")
        print(f"    {icon} {result.get('recommended_action')}  |  "
              f"Validation score: {result.get('validation_score')}  |  "
              f"{result.get('reason', '')}")

    with open("validation_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved {len(results)} validation results → validation_results.json")


if __name__ == "__main__":
    run()
