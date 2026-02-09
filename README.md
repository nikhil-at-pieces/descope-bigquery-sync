# Descope → BigQuery Daily Sync Cloud Function

This Cloud Function automatically syncs user data from Descope to BigQuery daily.

## Features

### Data Sync
- ✅ **Incremental Sync**: Uses `fromModifiedTime` to fetch only new/updated users
- ✅ **MERGE Logic**: Automatically handles inserts (new users) and updates (modified users)
- ✅ **No Duplicates**: MERGE on `user_id` ensures no duplicate entries
- ✅ **Location Enrichment**: Updates user login locations from audit events
- ✅ **IP Geolocation**: Enriches IP addresses with city/region data
- ✅ **Scheduled**: Runs automatically every day at 2 AM UTC
- ✅ **Efficient**: Collects all changes and performs one MERGE operation

### Security
- 🔒 **Secrets in Secret Manager**: No credentials in code or environment variables
- 🚫 **Private Function**: No public access, only Cloud Scheduler can trigger
- 🎫 **Authentication Required**: All requests validated
- 🔑 **Least Privilege**: Service accounts with minimal permissions
- 📝 **Audit Logging**: All activity logged and traceable

## Architecture

```
Cloud Scheduler (Daily 2 AM UTC)
    ↓
Cloud Function (HTTP Trigger)
    ↓
Descope API (Search Users with fromModifiedTime)
    ↓
BigQuery MERGE (Upsert users)
```

## Deployment

### Prerequisites

You need these IAM roles (request from your admin):
- `roles/cloudfunctions.developer` - Deploy functions
- `roles/iam.serviceAccountUser` - Run with service account  
- `roles/cloudscheduler.admin` - Create scheduler jobs
- `roles/serviceusage.serviceUsageConsumer` - Use APIs

### Secure Deployment (Recommended)

```bash
# From repo root
chmod +x deploy_secure.sh setup_secrets.sh
./setup_secrets.sh   # Once: store Descope credentials in Secret Manager (prompts or env)
./deploy_secure.sh
```

This will:
1. Create secrets in Google Secret Manager (if not done; use `setup_secrets.sh` first)
2. Deploy the Cloud Function to `us-central1` (private, no public access)
3. Configure service accounts with least-privilege permissions
4. Create a Cloud Scheduler job that runs daily at 2 AM UTC
5. Set up authentication and request validation

### Quick Deploy (Less Secure)

For development/testing only:
```bash
chmod +x deploy.sh
./deploy.sh
```

⚠️ **Note**: This uses environment variables and allows public access. Use `deploy_secure.sh` for production.

### Manual Testing

```bash
# Test the function manually (requires auth)
gcloud functions call descope-bigquery-sync \
  --gen2 \
  --region=us-central1 \
  --project=global-cloud-runtime

# Or trigger via scheduler
gcloud scheduler jobs run descope-sync-daily --location=us-central1 --project=global-cloud-runtime
```

### View Logs

```bash
# Real-time logs
gcloud functions logs read descope-bigquery-sync \
  --gen2 \
  --region=us-central1 \
  --project=global-cloud-runtime \
  --limit=50

# Or in Cloud Console
# https://console.cloud.google.com/functions/details/us-central1/descope-bigquery-sync
```

### Scheduler Management

```bash
# List jobs
gcloud scheduler jobs list --project=global-cloud-runtime

# Manually trigger the job
gcloud scheduler jobs run descope-sync-daily \
  --location=us-central1 \
  --project=global-cloud-runtime

# View job details
gcloud scheduler jobs describe descope-sync-daily \
  --location=us-central1 \
  --project=global-cloud-runtime

# Pause/Resume
gcloud scheduler jobs pause descope-sync-daily --location=us-central1 --project=global-cloud-runtime
gcloud scheduler jobs resume descope-sync-daily --location=us-central1 --project=global-cloud-runtime
```

## How It Works

1. **Gets Latest Timestamp**: Queries BigQuery for `MAX(created_time)`
2. **Fetches Changes**: Calls Descope API with `fromModifiedTime` filter
3. **Collects All Pages**: Paginates through all results (5000 per page)
4. **Single MERGE**: Performs one MERGE operation with all collected users
5. **Updates BigQuery**: 
   - Existing users → UPDATE
   - New users → INSERT
   - No duplicates!

## Configuration

- **Secrets**: Use `setup_secrets.sh` to store `descope-project-id` and `descope-management-key` in Secret Manager (no credentials in code).
- **Env (optional)**: For deploy scripts, set `GCP_PROJECT`. For BigQuery dataset/table override: `BQ_DATASET_USERS`, `BQ_TABLE_USERS`. See `.env.example`.

## Cost Estimate

- Cloud Function: ~$0.10/month (1 invocation/day, ~30s runtime)
- Cloud Scheduler: $0.10/month (1 job)
- **Total: ~$0.20/month**

## Troubleshooting

### Permission Denied

If you get permission errors, make sure you have the required IAM roles (see Prerequisites).

### Function Timeout

If syncing takes > 9 minutes, increase timeout:
```bash
gcloud functions deploy descope-bigquery-sync \
  --gen2 \
  --timeout=540s \
  ...
```

### Check BigQuery

```bash
bq query --nouse_legacy_sql \
  'SELECT MAX(created_time) as latest, COUNT(*) as total 
   FROM `global-cloud-runtime.descope_data_v2.users`'
```

## Modifying the Schedule

Edit the cron expression in `deploy.sh`:
- `"0 2 * * *"` = Daily at 2 AM UTC
- `"0 */6 * * *"` = Every 6 hours
- `"0 0 * * 0"` = Weekly on Sunday at midnight

Then redeploy with `./deploy.sh`.

