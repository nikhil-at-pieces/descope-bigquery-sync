# Descope ↔ BigQuery Sync — Handoff Doc

Authoritative reference for what is in production and how to operate it.

---

## 1. What is in production (authoritative)

### Descope → BigQuery Sync

**Primary and only confirmed production service.**

| Item | Details |
|------|---------|
| **Service type** | Google Cloud Function (Gen 2) |
| **Function name** | `descope-bigquery-sync` |
| **Trigger** | HTTP (invoked by Cloud Scheduler only) |
| **GCP project** | `global-cloud-runtime` |
| **Region** | `us-central1` |
| **Function URL** | https://us-central1-global-cloud-runtime.cloudfunctions.net/descope-bigquery-sync |
| **Schedule** | Daily at **02:00 AM UTC** |
| **Scheduler job** | `descope-sync-daily` |

### What the service does

- Syncs user records from Descope (identity provider) into BigQuery.
- Uses incremental fetching (`fromModifiedTime`) to pull only new or updated users.
- Performs upserts using a BigQuery MERGE on `user_id` (no duplicates).
- Optional enrichments (enabled in code):
  - Login location data from Descope audit logs.
  - IP-based geolocation (city, region, country).
- Credentials are **not** stored in code; they are pulled from **Google Secret Manager**.

### Secrets & authentication

| Secret | Location |
|--------|----------|
| `descope-project-id` | Google Secret Manager |
| `descope-management-key` | Google Secret Manager |

### Where the data lands

- **Dataset:** `global-cloud-runtime.descope_data_v2`
- **Table:** `users`

This table is used downstream for:

- GA4 attribution enrichment
- Cleaned user views (`users_clean`)
- Reporting and dashboards

### Deployment & CI/CD

- **Deployment method:** GitHub Actions (optional) or manual `./deploy_secure.sh`.
- **Source code (this repo):** `main.py` at repo root.
- **Entry point:** `descope_sync`.
- **Workflow (optional):** `.github/workflows/deploy.yml` — deploys from root, updates scheduler if needed.

---

## 2. Other pipelines (not in this repo)

The following run in the **company data-platform repo** (not here); they are run manually unless otherwise automated:

| Component | Location (company repo) | Purpose |
|-----------|--------------------------|---------|
| Social ingestion | `comb.py` | Twitter, YouTube, LinkedIn, TikTok → BigQuery (Apache Beam) |
| LinkedIn posts | `linkedin_pipeline.py` | LinkedIn company posts → BigQuery |
| GA4 attribution | `enrich_attribution_clean.py` | Adds GA4 first-touch attribution to users |
| IP geolocation | `enrich_ip_locations_fast.py` | Maps IP → city/region/country |
| YouTube Analytics | `youtube_analytics_to_bq.py` | YouTube Analytics → BigQuery |

---

## 3. How to operate the production sync

### Confirm it’s running

- **Cloud Function:** Cloud Console → Cloud Functions → `descope-bigquery-sync`  
  Project: `global-cloud-runtime`, Region: `us-central1`
- **Cloud Scheduler:** Cloud Console → Cloud Scheduler → job `descope-sync-daily`

### View logs

```bash
gcloud functions logs read descope-bigquery-sync \
  --gen2 \
  --region=us-central1 \
  --project=global-cloud-runtime \
  --limit=50
```

Or: Cloud Console → Logs Explorer (filter by function name).

### Manually trigger a run

```bash
gcloud scheduler jobs run descope-sync-daily \
  --location=us-central1 \
  --project=global-cloud-runtime
```

⚠️ The function is not intended to be called directly from the public internet. Use Cloud Scheduler or authenticated calls only.

### Validate data in BigQuery

```bash
bq query --nouse_legacy_sql \
  'SELECT MAX(created_time) AS latest, COUNT(*) AS total
   FROM `global-cloud-runtime.descope_data_v2.users`'
```

### Change the schedule

- Cloud Console → Cloud Scheduler → `descope-sync-daily` → Edit  
- Or redeploy and update the job (e.g. via `deploy_secure.sh` or CI).

### Pause / resume the job

**Pause:**

```bash
gcloud scheduler jobs pause descope-sync-daily \
  --location=us-central1 \
  --project=global-cloud-runtime
```

**Resume:**

```bash
gcloud scheduler jobs resume descope-sync-daily \
  --location=us-central1 \
  --project=global-cloud-runtime
```

---

## 4. Dependencies & required access

- **GCP project:** `global-cloud-runtime`
- **BigQuery:** Dataset `descope_data_v2`, table `users`
- **Cloud Scheduler:** Job must stay authenticated to the function. If the function URL or auth changes, update the scheduler job.
- **Secret Manager:** Secrets `descope-project-id` and `descope-management-key` must exist and be accessible by the function’s service account.

---

## 5. Making changes & redeploying

- **Code:** `main.py` (and any supporting files in this repo).
- **Deploy:** Run `./deploy_secure.sh` from repo root, or use GitHub Actions if configured.
- **Config:** `GCP_PROJECT` set in workflow or deploy script. Dataset/table in code (env overrides optional). Descope credentials from Secret Manager only.

---

## 6. Repository & links

- **This repo:** https://github.com/nikhil-at-pieces/descope-bigquery-sync  
- **Production function URL:** https://us-central1-global-cloud-runtime.cloudfunctions.net/descope-bigquery-sync  
- **Main README:** [README.md](README.md) — project overview and local setup.
