# Quick Fix for Scheduler Authentication

## The Problem

Cloud Function is **working fine**, but Cloud Scheduler can't call it because it's missing authentication configuration.

**Error seen in logs:**
```
The request was not authenticated.
```

## The Fix (30 seconds)

Run this script as admin/CTO:

```bash
cd cloud_function
./fix-scheduler-auth.sh
```

**That's it!** The script will:
1. Delete the broken scheduler job
2. Recreate it with proper OIDC authentication
3. Test it to verify it works

## What Happened?

Your Cloud Function is deployed in **secure mode** (good for security ✅), but Cloud Scheduler was configured without the authentication token it needs to call the function.

## What I Already Fixed

✅ IAM permissions are now correct  
✅ Service accounts have proper roles  
✅ Function is healthy and ready  

⚠️ Just needs: Cloud Scheduler job recreated with OIDC auth

## Why Can't I Fix It?

I don't have `iam.serviceAccounts.actAs` permission. This requires admin/CTO to run the fix script.

## Verification

After running the fix, check logs:

```bash
gcloud functions logs read descope-bigquery-sync \
  --gen2 --region=us-central1 --project=global-cloud-runtime --limit=10
```

You should see:
```
✅ Starting Descope incremental sync...
✅ Sync completed! Processed X users
```

Instead of:
```
❌ The request was not authenticated...
```

## Alternative: Grant Me Permission

If you want me to fix it, CTO can run:

```bash
gcloud iam service-accounts add-iam-policy-binding \
  descope-sync-sa@global-cloud-runtime.iam.gserviceaccount.com \
  --member="user:nikhil@pieces.app" \
  --role="roles/iam.serviceAccountUser" \
  --project=global-cloud-runtime
```

Then I can run the fix myself.

**Note:** You might already have this permission since you deployed the function with this service account!

---

**Full details:** See `SCHEDULER_AUTH_FIX.md`

