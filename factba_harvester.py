import os
import sys
import json
import time
import logging
import requests

# Configure real-time computational pipeline logging visibility
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Workspace Path Definitions
MASTER_JSONL_PATH = "trump_complete_archive.jsonl"
TWITTER_JSON_PATH = "data/twitter_archive.json"
TRUTH_SOCIAL_JSON_PATH = "data/truth_social_archive.json"
FACTBASE_API_URL = "https://rollcall.com/wp-json/factbase/v1/twitter"

STANDARD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json"
}

# =========================================================================
# MODULE 1: THE UNIFIED GLOBAL SCRAPER
# =========================================================================
def run_unified_global_scraper(max_pages=10):
    """
    Pass 1: Connects to the global index (platform=all) and streams 
    the raw text data directly onto the disk ledger log file.
    """
    logging.info("==================================================")
    logging.info("🚀 PHASE 1: INITIALIZING UNIFIED GLOBAL HARVESTER")
    logging.info("==================================================")
    
    current_page = 1
    total_downloaded = 0
    
    # Clean or initialize the master log storage file layout
    if os.path.exists(MASTER_JSONL_PATH):
        logging.info(f"♻️ Existing log file detected at '{MASTER_JSONL_PATH}'. Appending fresh stream deltas...")
    
    while current_page <= max_pages:
        params = {
            "platform": "all",
            "sort": "date",
            "sort_order": "desc",
            "page": str(current_page)
        }
        
        try:
            logging.info(f"📡 Requesting live data stream page {current_page}...")
            response = requests.get(FACTBASE_API_URL, headers=STANDARD_HEADERS, params=params, timeout=15)
            
            # A 404 response marks that we have pushed past historical tail boundaries
            if response.status_code == 404:
                logging.info(f"✓ Historical tail boundaries reached at page {current_page-1}.")
                break
                
            response.raise_for_status()
            payload = response.json()
            posts = payload.get("data", []) if isinstance(payload, dict) else payload
            
            if not posts:
                logging.info(f"✓ Data stream returned empty on page {current_page-1}. Stopping scrape.")
                break
                
            # Stream the raw lines natively into the JSONL master warehouse tracking layer
            with open(MASTER_JSONL_PATH, "a", encoding="utf-8") as f:
                for post in posts:
                    f.write(json.dumps(post, ensure_ascii=False) + "\n")
                    total_downloaded += 1
                    
            current_page += 1
            time.sleep(1.0)  # Polite server response pacing buffer
            
        except Exception as e:
            logging.error(f"✗ Network harvest pass failed on page {current_page}: {e}")
            logging.info("Waiting 5 seconds before retrying stream...")
            time.sleep(5.0)
            continue
            
    logging.info(f"🎉 Phase 1 finished! Scraped and cached {total_downloaded} entries into '{MASTER_JSONL_PATH}'.")


# =========================================================================
# MODULE 2: THE DATA MATRIX SPLITTER & METRICS LOOP
# =========================================================================
def run_archive_matrix_splitter():
    """
    Pass 2: Reads the fresh master .jsonl file, routes records by platform 
    origin, completes zero-cost metrics for Truth Social, and writes splits to data/.
    """
    logging.info("==================================================")
    logging.info("⚙️ PHASE 2: INITIALIZING DATA DIVISION METRICS LOOP")
    logging.info("==================================================")
    
    if not os.path.exists(MASTER_JSONL_PATH):
        logging.error(f"🚨 Cannot run data division pass. Master file missing at '{MASTER_JSONL_PATH}'")
        return

    raw_twitter_pool = []
    raw_truth_pool = []
    
    # Read the master raw database cleanly from your hard drive
    with open(MASTER_JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                post = json.loads(line)
                platform_raw = str(post.get("platform", "")).lower()
                record_id = str(post.get("document_id") or post.get("id", ""))
                
                # Standardize database property parameters
                base_schema = {
                    "id": record_id,
                    "timestamp_utc": post.get("timestamp") or post.get("date", ""),
                    "text": (post.get("text") or "").strip(),
                    "url": post.get("permalink") or post.get("post_url") or "",
                    "metrics": {"likes": 0, "reposts_retruths": 0, "replies": 0}
                }
                
                # Map record targets based on source infrastructure signatures
                if "truth" in platform_raw:
                    raw_truth_pool.append(base_schema)
                else:
                    raw_twitter_pool.append(base_schema)
            except Exception:
                continue

    os.makedirs("data", exist_ok=True)

    # ─── SECTION A: PROCESS TRUTH SOCIAL SECTOR WITH ZERO-COST LIVE METRICS ───
    if raw_truth_pool:
        logging.info(f"📡 Completing live interaction indicators for {len(raw_truth_pool)} Truth Social items...")
        completed_truth_count = 0
        
        for idx, item in enumerate(raw_truth_pool):
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
                    completed_truth_count += 1
                if idx % 20 == 0 and idx > 0:
                    logging.info(f"   ✓ Telemetry matched for {idx}/{len(raw_truth_pool)} entries...")
                time.sleep(0.3)  # Gentle pacing buffer for open server endpoints
            except Exception:
                continue
                
        with open(TRUTH_SOCIAL_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(raw_truth_pool, f, indent=2, ensure_ascii=False)
        logging.info(f"🎉 Truth Social Split Matrix locked! Saved to '{TRUTH_SOCIAL_JSON_PATH}'")

    # ─── SECTION B: PROCESS X-TWITTER SECTOR (TEXT-ONLY REPLICATION CAPACITIES) ───
    if raw_twitter_pool:
        # We explicitly omit live Twitter metrics lookups to bypass Developer API limit costs
        with open(TWITTER_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(raw_twitter_pool, f, indent=2, ensure_ascii=False)
        logging.info(f"🎉 X-Twitter Split Matrix locked! Saved to '{TWITTER_JSON_PATH}'")


# =========================================================================
# MODULE 3: PIPELINE MAIN CONTROLLER
# =========================================================================
def run_all_pipeline_stages():
    """
    Pass 3: Orchestrates execution blocks in order to guarantee data integrity.
    """
    start_time = time.time()
    
    # 1. Run the live internet harvest sweep
    # (Set max_pages high to scrape the full historic timeline, e.g., max_pages=5000)
    run_unified_global_scraper(max_pages=5)
    
    # 2. Split the master log file and enrich live elements
    run_archive_matrix_splitter()
    
    elapsed_time = time.time() - start_time
    logging.info("==================================================")
    logging.info(f"🏁 PIPELINE RUN COMPLETE | Total Processing Time: {elapsed_time:.2f}s")
    logging.info("==================================================")

if __name__ == "__main__":
    run_all_pipeline_stages()