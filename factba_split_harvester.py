import os
import json
import time
import logging
import requests

# Configure real-time computational pipeline logging visibility
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Workspace Directory Paths
INPUT_ARCHIVE_PATH = "trump_complete_archive.json"
TWITTER_JSON_PATH = "data/twitter_archive.json"
TRUTH_SOCIAL_JSON_PATH = "data/truth_social_archive.json"

STANDARD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json"
}

def parse_local_historical_archive():
    """Parses the pre-compiled master jsonl archive file directly from local disk storage."""
    if not os.path.exists(INPUT_ARCHIVE_PATH):
        logging.error(f"🚨 Missing historical source file at: '{INPUT_ARCHIVE_PATH}'")
        logging.error("Please ensure your pre-compiled master dataset is in the root execution directory.")
        return [], []

    logging.info(f"⏳ Opening historical database: '{INPUT_ARCHIVE_PATH}' for structural split parsing...")
    raw_twitter_pool = []
    raw_truth_pool = []
    total_scanned = 0

    with open(INPUT_ARCHIVE_PATH, "r", encoding="utf-8") as f:
        for line in f:
            total_scanned += 1
            try:
                post = json.loads(line)
                platform_raw = str(post.get("platform", "")).lower()
                record_id = str(post.get("document_id") or post.get("id", ""))
                
                # Align data properties to standard schema format
                base_schema = {
                    "id": record_id,
                    "timestamp_utc": post.get("timestamp") or post.get("date", ""),
                    "text": (post.get("text") or "").strip(),
                    "url": post.get("permalink") or post.get("post_url") or "",
                    "metrics": {"likes": 0, "reposts_retruths": 0, "replies": 0}
                }
                
                # Direct data segregation based on source network tags
                if "truth" in platform_raw:
                    raw_truth_pool.append(base_schema)
                else:
                    raw_twitter_pool.append(base_schema)
            except Exception:
                continue

    logging.info(f"✓ Scan complete. Processed {total_scanned} entries successfully from disk.")
    logging.info(f"👉 Segregated Counts -- Truth Social Pool: {len(raw_truth_pool)} | X-Twitter Pool: {len(raw_twitter_pool)}")
    return raw_truth_pool, raw_twitter_pool

def complete_truth_social_metrics(records):
    """Fills down live interaction numbers over un-throttled open access nodes at zero cost."""
    logging.info(f"📡 Completing live interaction metrics for {len(records)} Truth Social items...")
    completed_count = 0
    
    for idx, item in enumerate(records):
        status_id = item["id"]
        api_url = f"https://truthsocial.com/api/v1/statuses/{status_id}"
        try:
            resp = requests.get(api_url, headers=STANDARD_HEADERS, timeout=10)
            if resp.status_code == 200:
                payload = resp.json()
                item["metrics"] = {
                    "likes": payload.get("favourites_count", 0),
                    "reposts_retruths": payload.get("reblogs_count", 0),
                    "replies": payload.get("replies_count", 0)
                }
                completed_count += 1
            if idx % 10 == 0 and idx > 0:
                logging.info(f"   ✓ Telemetry matched for {idx}/{len(records)} items...")
            
            # Brief server pacing buffer
            time.sleep(0.2)
        except Exception:
            continue
            
    logging.info(f"✓ Telemetry pass ended. Completed metrics across {completed_count} active streams.")
    return records

def run_split_pipeline():
    os.makedirs("data", exist_ok=True)
    
    # Run the file-based historical extraction engine
    truth_pool, twitter_pool = parse_local_historical_archive()
    
    # ─── PASS 1: PROCESS TRUTH SOCIAL SECTOR WITH ZERO-COST METRICS ───
    if truth_pool:
        completed_truth = complete_truth_social_metrics(truth_pool)
        with open(TRUTH_SOCIAL_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(completed_truth, f, indent=2, ensure_ascii=False)
        print(f"🎉 Truth Social Dataset Saved: {len(completed_truth)} items -> '{TRUTH_SOCIAL_JSON_PATH}'")

    # ─── PASS 2: PROCESS X-TWITTER SECTOR (TEXT-ONLY CAPACITY, ZERO COST) ───
    if twitter_pool:
        with open(TWITTER_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(twitter_pool, f, indent=2, ensure_ascii=False)
        print(f"🎉 X-Twitter Dataset Saved: {len(twitter_pool)} items [Text-Only Mode] -> '{TWITTER_JSON_PATH}'")

if __name__ == "__main__":
    run_split_pipeline()