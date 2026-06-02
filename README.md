# 🇺🇸 Trump Executive Communication Stream Split Dataset (2026 Archive)

This repository functions as a persistent, open-science data architecture housing unmediated executive crisis communications generated during the 2026 Iran War conflict timeline. 

## 📦 Data Architecture Partitions

- `data/truth_social_archive.json`: Full historical segment populated with live, zero-cost interaction telemetries (likes, re-truths, replies) extracted over open public nodes[cite: 29].
- `data/twitter_archive.json`: Full historical text footprint archive. To completely safeguard repository users from unsustainable developer token limits and plan expenses, this file functions in a text-only replication capacity[cite: 29].

## 🛠️ Replication & Independent Audit Guide

To clone this workspace, initialize your virtual environment, and run a full dataset split compilation directly from the historical archive file, execute the following commands[cite: 29]:

```bash
# 1. Clone the split repository framework
git clone [https://github.com/your-profile/trump-social-split-archive.git](https://github.com/your-profile/trump-social-split-archive.git)
cd trump-social-split-archive

# 2. Build insulated local virtual environment space and dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Trigger the disk-based split harvest processing loop
python factba_split_harvester.py
```

