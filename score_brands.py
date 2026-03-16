import sqlite3
import math
import json
from datetime import datetime, timedelta, timezone
from brand_ticker_map import get_ticker, normalize

DB_PATH = "marketpulse.db"


def normalize_brand(brand: str) -> str:
    return normalize(brand) if brand else ""


def load_signals(conn: sqlite3.Connection, days: int = 30) -> list[dict]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT s.brand_or_product, s.signal_type, s.confidence, s.intensity,
               s.signal_detected, p.timestamp, p.platform
        FROM signals s
        JOIN raw_posts p ON s.post_id = p.id
        WHERE s.signal_detected = 1
          AND p.timestamp >= ?
    """, (cutoff,))
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def calculate_momentum_score(signals: list[dict], brand: str) -> dict:
    brand_signals = [s for s in signals if normalize_brand(s["brand_or_product"]) == normalize_brand(brand)]

    if not brand_signals:
        return {"brand": brand, "score": 0, "signal_count": 0}

    now = datetime.now(timezone.utc)

    # --- Volume Score (0–25) ---
    count_7d = sum(1 for s in brand_signals
                   if s["timestamp"] and
                   datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")) >= now - timedelta(days=7))
    volume_score = min(25, math.log1p(count_7d) * 8)

    # --- Velocity Score (0–25): week-over-week growth ---
    count_prev_week = sum(1 for s in brand_signals
                          if s["timestamp"] and
                          now - timedelta(days=14) <=
                          datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")) <
                          now - timedelta(days=7))
    if count_prev_week > 0:
        growth = (count_7d - count_prev_week) / count_prev_week
        velocity_score = min(25, max(0, growth * 25))
    else:
        velocity_score = min(25, count_7d * 5)  # no prior week → treat as new signal

    # --- Depth Score (0–25): avg confidence × intensity weight ---
    intensity_weights = {"low": 0.3, "medium": 0.6, "high": 1.0}
    depth_scores = []
    for s in brand_signals:
        conf = s["confidence"] or 0.5
        iw = intensity_weights.get(s["intensity"] or "medium", 0.6)
        depth_scores.append(conf * iw)
    depth_score = min(25, (sum(depth_scores) / len(depth_scores)) * 25) if depth_scores else 0

    # --- Cross-Platform Score (0–25) ---
    platforms = {s["platform"] for s in brand_signals if s["platform"]}
    cross_platform_score = min(25, len(platforms) * 12.5)

    total = volume_score + velocity_score + depth_score + cross_platform_score

    # --- Breakout Detection ---
    # Score jumps 15+ pts in 48 hrs → BREAKOUT
    count_48h = sum(1 for s in brand_signals
                    if s["timestamp"] and
                    datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")) >= now - timedelta(hours=48))
    count_48h_prior = sum(1 for s in brand_signals
                          if s["timestamp"] and
                          now - timedelta(hours=96) <=
                          datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")) <
                          now - timedelta(hours=48))
    score_48h = min(25, math.log1p(count_48h) * 8)
    score_48h_prior = min(25, math.log1p(count_48h_prior) * 8)
    breakout = (score_48h - score_48h_prior) >= 15

    return {
        "brand": brand,
        "ticker": get_ticker(brand),
        "score": round(total, 1),
        "volume_score": round(volume_score, 1),
        "velocity_score": round(velocity_score, 1),
        "depth_score": round(depth_score, 1),
        "cross_platform_score": round(cross_platform_score, 1),
        "signal_count": len(brand_signals),
        "signal_count_7d": count_7d,
        "breakout": breakout,
        "dominant_signal_type": max(
            set(s["signal_type"] for s in brand_signals if s["signal_type"]),
            key=lambda t: sum(1 for s in brand_signals if s["signal_type"] == t),
            default=None,
        ),
        "top_trigger": next(
            (s["trigger_phrase"] for s in sorted(brand_signals,
             key=lambda x: x["confidence"] or 0, reverse=True)
             if s.get("trigger_phrase")), None
        ),
    }


def run():
    conn = sqlite3.connect(DB_PATH)
    signals = load_signals(conn, days=30)
    conn.close()

    if not signals:
        print("No signals found. Run classify_signals.py first.")
        return

    # collect all unique brands mentioned
    brands_seen = set()
    for s in signals:
        if s["brand_or_product"]:
            # some posts list multiple brands separated by commas
            for b in s["brand_or_product"].split(","):
                b = b.strip()
                if b:
                    brands_seen.add(b)

    print(f"Scoring {len(brands_seen)} unique brands...\n")

    results = []
    for brand in brands_seen:
        score = calculate_momentum_score(signals, brand)
        if score["signal_count"] > 0:
            results.append(score)

    results.sort(key=lambda x: x["score"], reverse=True)

    print(f"{'Brand':<40} {'Ticker':<8} {'Score':>6}  {'Vol':>5} {'Vel':>5} {'Dep':>5} {'X-Plt':>5}  {'Signals':>7}")
    print("-" * 95)
    for r in results[:30]:
        ticker = r["ticker"] or "—"
        print(f"{r['brand']:<40} {ticker:<8} {r['score']:>6.1f}  "
              f"{r['volume_score']:>5.1f} {r['velocity_score']:>5.1f} "
              f"{r['depth_score']:>5.1f} {r['cross_platform_score']:>5.1f}  "
              f"{r['signal_count']:>7}")

    # save to JSON for Layer 4
    with open("brand_scores.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved {len(results)} brand scores → brand_scores.json")


if __name__ == "__main__":
    run()
