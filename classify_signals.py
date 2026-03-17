import sqlite3
import json
import time
import os
from dotenv import load_dotenv
import anthropic

load_dotenv()

DB_PATH = "marketpulse.db"
BATCH_SIZE = 500  # safe daily budget limit

# Pre-filter: only call the LLM if post contains at least one of these keywords.
# Cuts API calls by ~60% by skipping clearly irrelevant posts.
SIGNAL_KEYWORDS = [
    "obsessed", "can't stop", "addicted", "love it", "amazing",
    "switched", "switch", "replaced", "used to use", "used to buy",
    "everyone", "all my friends", "everywhere",
    "just found", "hidden gem", "nobody talks about", "underrated",
    "worth every penny", "spent too much", "keep coming back", "buying again",
    "bought", "purchase", "recommend", "review", "honest", "worth it",
    "best", "favourite", "favorite", "daily driver", "daily use",
    "game changer", "life changing", "can't live without",
]

SYSTEM_PROMPT = """You are a consumer signal analyst. Your job is to read social media posts and detect whether they contain one of five consumer signal patterns that may have financial market implications.

The five signal types:
1. OBSESSION — "I can't stop buying this", "I have 4 of these", "I'm obsessed" → strong demand, potential revenue beat
2. SOCIAL_PROOF — "Everyone has one", "it's everywhere", "all my friends use" → mass adoption, brand momentum
3. SWITCHING — "I switched from X to Y", "I used to use X but now" → brand rotation, market share shift
4. DISCOVERY — "Just found this", "nobody is talking about this yet", "hidden gem" → early trend, pre-mainstream
5. SPEND_CONFESSION — "Worth every penny", "I spent way too much on", "I keep coming back" → pricing power, high willingness to pay

Return ONLY valid JSON. No explanation, no markdown.

Output format:
{
  "signal_detected": true or false,
  "signal_type": "OBSESSION" | "SOCIAL_PROOF" | "SWITCHING" | "DISCOVERY" | "SPEND_CONFESSION" | null,
  "confidence": 0.0 to 1.0,
  "intensity": "low" | "medium" | "high" | null,
  "brand_or_product": "exact brand or product name" | null,
  "ticker_hint": "stock ticker if publicly traded" | null,
  "trigger_phrase": "the exact phrase that triggered the signal" | null,
  "market_implication": "one sentence max" | null
}

If no signal is detected, return signal_detected: false and null for all other fields except confidence (set to 0.0)."""


def classify_post(client: anthropic.Anthropic, text: str) -> dict:
    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            messages=[
                {
                    "role": "user",
                    "content": f"Classify this post:\n\n{text[:500]}"  # cap at 500 chars
                }
            ],
            system=SYSTEM_PROMPT,
        )
        raw = message.content[0].text.strip()
        # strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  [!] JSON parse error: {e}")
        return None
    except Exception as e:
        print(f"  [!] API error: {e}")
        return None


def save_signal(cursor: sqlite3.Cursor, post_id: int, result: dict):
    cursor.execute(
        """
        INSERT INTO signals (
            post_id, signal_detected, signal_type, confidence, intensity,
            brand_or_product, ticker_hint, trigger_phrase, market_implication
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            post_id,
            1 if result.get("signal_detected") else 0,
            result.get("signal_type"),
            result.get("confidence"),
            result.get("intensity"),
            result.get("brand_or_product"),
            result.get("ticker_hint"),
            result.get("trigger_phrase"),
            result.get("market_implication"),
        ),
    )


def run():
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # fetch unprocessed tier1 posts only — tier2 is institutional media, used for gap detection
    cursor.execute(
        """SELECT id, text, video_title FROM raw_posts
           WHERE processed = 0 AND (source_tier = 'tier1' OR source_tier IS NULL)
           LIMIT ?""",
        (BATCH_SIZE,)
    )
    posts = cursor.fetchall()

    if not posts:
        print("No unprocessed posts found.")
        conn.close()
        return

    # Pre-filter: skip posts with no signal keywords — no API call needed
    def has_keywords(text: str) -> bool:
        t = text.lower()
        return any(kw in t for kw in SIGNAL_KEYWORDS)

    filtered = [(pid, txt, vtitle) for pid, txt, vtitle in posts if has_keywords(txt)]
    skipped = len(posts) - len(filtered)

    # Mark skipped posts as processed without creating a signal row
    skip_ids = [pid for pid, txt, vtitle in posts if not has_keywords(txt)]
    for pid in skip_ids:
        cursor.execute("UPDATE raw_posts SET processed = 1 WHERE id = ?", (pid,))
    conn.commit()

    print(f"Pre-filter: {skipped} posts skipped (no keywords), {len(filtered)} sent to LLM\n")
    signals_found = 0

    for post_id, text, video_title in filtered:
        # Prepend video title so the LLM has brand/topic context
        if video_title:
            llm_input = f"[Video: {video_title}] {text}"
        else:
            llm_input = text
        result = classify_post(client, llm_input)

        if result is None:
            continue

        save_signal(cursor, post_id, result)

        # mark post as processed
        cursor.execute("UPDATE raw_posts SET processed = 1 WHERE id = ?", (post_id,))
        conn.commit()

        if result.get("signal_detected"):
            signals_found += 1
            print(f"  [SIGNAL] Post {post_id} | {result.get('signal_type')} | "
                  f"confidence={result.get('confidence'):.2f} | "
                  f"brand={result.get('brand_or_product')} | "
                  f"\"{result.get('trigger_phrase')}\"")
        else:
            print(f"  [none]   Post {post_id}")

        time.sleep(0.3)  # avoid rate limits

    conn.close()
    print(f"\nDone. {signals_found}/{len(posts)} posts had signals.")


if __name__ == "__main__":
    run()
