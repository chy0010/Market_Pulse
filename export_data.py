"""
Export signals_data.json and metadata.json from the SQLite DB.
Run after classify_signals.py in the pipeline.
"""
import json
import sqlite3
from datetime import datetime, timezone

DB_PATH = "marketpulse.db"


def run():
    conn = sqlite3.connect(DB_PATH)

    # Export signals
    rows = conn.execute("""
        SELECT s.id, s.signal_type, s.confidence, s.intensity,
               s.brand_or_product, s.ticker_hint, s.trigger_phrase,
               s.market_implication, s.classified_at,
               p.text, p.platform, p.source, p.timestamp
        FROM signals s
        JOIN raw_posts p ON s.post_id = p.id
        WHERE s.signal_detected = 1
          AND p.platform != 'reddit'
        ORDER BY s.classified_at DESC
    """).fetchall()

    cols = ["id", "signal_type", "confidence", "intensity", "brand_or_product",
            "ticker_hint", "trigger_phrase", "market_implication", "classified_at",
            "text", "platform", "source", "timestamp"]
    signals = [dict(zip(cols, r)) for r in rows]

    with open("signals_data.json", "w") as f:
        json.dump(signals, f, indent=2)
    print(f"Exported {len(signals)} signals to signals_data.json")

    # Export metadata
    post_count = conn.execute(
        "SELECT COUNT(*) FROM raw_posts WHERE platform != 'reddit'"
    ).fetchone()[0]

    metadata = {
        "post_count": post_count,
        "signal_count": len(signals),
        "exported_at": datetime.now(timezone.utc).isoformat(),
    }
    with open("metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Exported metadata: {post_count} posts, {len(signals)} signals")

    conn.close()


if __name__ == "__main__":
    run()
