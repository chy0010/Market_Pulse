import sqlite3
import os

DB_PATH = "marketpulse.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()


cursor.execute("""
CREATE TABLE IF NOT EXISTS raw_posts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    text        TEXT NOT NULL,
    source      TEXT,
    platform    TEXT,
    timestamp   TEXT,
    processed   INTEGER DEFAULT 0,
    created_at  TEXT DEFAULT CURRENT_TIMESTAMP
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS signals (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id            INTEGER REFERENCES raw_posts(id),
    signal_detected    INTEGER,
    signal_type        TEXT,
    confidence         REAL,
    intensity          TEXT,
    brand_or_product   TEXT,
    ticker_hint        TEXT,
    trigger_phrase     TEXT,
    market_implication TEXT,
    classified_at      TEXT DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()
conn.close()

print(f"✓ Database created: {os.path.abspath(DB_PATH)}")
print("✓ Tables created: raw_posts, signals")