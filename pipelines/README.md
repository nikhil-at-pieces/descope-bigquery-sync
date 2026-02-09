# Other pipelines (same repo)

These scripts ingest and enrich data in BigQuery. They are **not** deployed as a service; run them manually or on your own schedule (cron, Cloud Scheduler, etc.). All config is via **environment variables**—no credentials or internal project IDs in code.

| Script | Purpose |
|--------|---------|
| **comb.py** | Twitter, YouTube, LinkedIn, TikTok → BigQuery (Apache Beam). Staging + MERGE upsert. |
| **linkedin_pipeline.py** | LinkedIn company posts → BigQuery. Incremental, rate-limit aware. |
| **enrich_attribution_clean.py** | GA4 first-touch attribution → users table. |
| **enrich_ip_locations_fast.py** | IP → city/region/country; updates users table. |
| **youtube_analytics_to_bq.py** | YouTube Analytics API → BigQuery (OAuth). |

## Setup

```bash
cd pipelines
pip install -r requirements.txt
cp .env.example .env   # then edit .env with your GCP project, datasets, API keys
```

Required env (see `.env.example`): `GCP_PROJECT`, `BQ_DATASET_ANALYTICS`, `BQ_DATASET_USERS` (for enrichment), plus Twitter/YouTube/LinkedIn keys as needed.

## Run

```bash
python comb.py
python linkedin_pipeline.py --mode incremental
python enrich_attribution_clean.py
python enrich_ip_locations_fast.py
python youtube_analytics_to_bq.py
```
