#!/bin/bash

# Fix Cloud Scheduler Authentication
# This script configures Cloud Scheduler to properly authenticate with the private Cloud Function
#
# Issue: Cloud Scheduler was calling the function without authentication
# Solution: Configure OIDC authentication with the scheduler service account

set -e

PROJECT_ID="global-cloud-runtime"
REGION="us-central1"
SCHEDULER_SA="scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com"
FUNCTION_NAME="descope-bigquery-sync"

echo "🔧 Fixing Cloud Scheduler Authentication"
echo "========================================="
echo ""

# Get function URL
echo "📍 Getting Cloud Function URL..."
FUNCTION_URL=$(gcloud functions describe ${FUNCTION_NAME} \
  --gen2 \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --format='value(serviceConfig.uri)')

echo "   Function URL: ${FUNCTION_URL}"
echo ""

# Delete existing scheduler job
echo "🗑️  Deleting existing scheduler job..."
gcloud scheduler jobs delete descope-sync-daily \
  --location=${REGION} \
  --project=${PROJECT_ID} \
  --quiet || echo "   Job doesn't exist, continuing..."

echo ""

# Create scheduler job with OIDC authentication
echo "✅ Creating scheduler job with OIDC authentication..."
gcloud scheduler jobs create http descope-sync-daily \
  --location=${REGION} \
  --schedule="0 2 * * *" \
  --uri="${FUNCTION_URL}" \
  --http-method=POST \
  --time-zone="UTC" \
  --project=${PROJECT_ID} \
  --description="Daily sync of Descope users to BigQuery" \
  --oidc-service-account-email="${SCHEDULER_SA}" \
  --oidc-token-audience="${FUNCTION_URL}"

echo ""
echo "========================================="
echo "✅ Fix Complete!"
echo "========================================="
echo ""
echo "Testing the fix..."
echo ""

# Test the scheduler
gcloud scheduler jobs run descope-sync-daily \
  --location=${REGION} \
  --project=${PROJECT_ID}

echo ""
echo "⏳ Waiting 10 seconds for function to execute..."
sleep 10

echo ""
echo "📋 Recent logs:"
gcloud functions logs read ${FUNCTION_NAME} \
  --gen2 \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --limit=5

echo ""
echo "========================================="
echo "✅ Scheduler is now configured with OIDC authentication!"
echo ""
echo "Next scheduled run: Daily at 2:00 AM UTC"
echo "========================================="

