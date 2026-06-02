# 🇺🇸 Trump Executive Social Media Stream Global Ingestion & Split Pipeline

This repository contains the high-throughput processing pipeline built to comprehensively ingest, archive, and segregate Donald J. Trump's social media broadcasts from Factba.se.

The system architecture features a **three-stage automated pipeline** that executes a unified global harvest stream (`platform=all`), dumps raw properties onto your disk layout, and creates segregated JSON output directories without incurring developer API account billing metrics or costs.

---

## 📦 Data Architecture Layout

- `**trump_complete_archive.jsonl`**: The single-source-of-truth log file on your disk. It caches the complete, raw multi-platform history directly from the ingestion passes before segregation occurs.
- `**data/truth_social_archive.json**`: The complete historical slice of Truth Social entries. Every entry is automatically completed with real-time interaction metrics (likes, reposts, and replies) extracted over zero-cost open endpoints.
- `**data/twitter_archive.json**`: The complete historical footprint of X-Twitter entries. To safeguard repository users from unsustainable developer token limits, billing constraints, and plan caps, this file operates in a text-only replication capacity.

---

## ⚙️ Three-Stage Execution Flow

The core workflow engine inside `factba_split_harvester.py` organizes processing blocks into the following sequence:

1. **Unified Ingestion Scraper (`run_unified_global_scraper`)**: Crawls backward across the global index stream (`platform=all`), pulling down mixed message components simultaneously and appending raw variables into the master log file.
2. **Matrix Splitter & Metrics Loop (`run_archive_matrix_splitter`)**: Processes your local disk log lines, separates entries by platform tags, runs metrics loops on Truth Social statuses via public nodes, and writes output partitions.
3. **Pipeline Orchestrator (`run_all_pipeline_stages`)**: Automatically controls initialization sequences, handles server pacing delays, and logs diagnostic reporting updates to verify dataset reproducibility.

---

## 🛠️ Replication & Implementation Guide

To stand up your virtual environment sandbox and execute the multi-platform database segmentation layout, run the following workflow sequence:

```bash
# 1. Clone the split repository framework
git clone [https://github.com/your-profile/trump-social-split-archive.git](https://github.com/your-profile/trump-social-split-archive.git)
cd trump-social-split-archive

# 2. Build local insulated virtual environment space and dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install requests python-dotenv

# 3. Trigger the three-stage split harvest processing loop
python factba_harvester.py
```

