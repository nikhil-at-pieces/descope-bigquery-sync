#!/bin/bash

# Configuration
PROJECT_ID="global-cloud-runtime"
REGION="us-central1"
FUNCTION_NAME="descope-bigquery-sync"
SERVICE_ACCOUNT="descope-sync-sa@${PROJECT_ID}.iam.gserviceaccount.com"
SCHEDULER_SA="scheduler-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🔒 Deploying Descope BigQuery Sync Cloud Function (SECURE MODE)..."
echo ""

# Step 1: Setup secrets if not already done
echo "📋 Step 1: Checking secrets..."
if ! gcloud secrets describe descope-project-id --project=${PROJECT_ID} &>/dev/null; then
    echo "⚠️  Secrets not found. Running setup_secrets.sh..."
    chmod +x setup_secrets.sh
    ./setup_secrets.sh
else
    echo "✅ Secrets already configured"
fi

echo ""
echo "📋 Step 2: Checking service accounts..."

# Check if function service account exists
if ! gcloud iam service-accounts describe ${SERVICE_ACCOUNT} --project=${PROJECT_ID} &>/dev/null; then
    echo "❌ Function service account not found. Creating it..."
    gcloud iam service-accounts create descope-sync-sa \
        --display-name="Descope BigQuery Sync Service Account" \
        --project=${PROJECT_ID}
    
    # Grant BigQuery permissions
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
        --member="serviceAccount:${SERVICE_ACCOUNT}" \
        --role="roles/bigquery.dataEditor"
    
    echo "✅ Service account created"
else
    echo "✅ Function service account exists"
fi

# Create scheduler service account if it doesn't exist
if ! gcloud iam service-accounts describe ${SCHEDULER_SA} --project=${PROJECT_ID} &>/dev/null; then
    echo "Creating scheduler service account..."
    gcloud iam service-accounts create scheduler-sa \
        --display-name="Cloud Scheduler Service Account" \
        --project=${PROJECT_ID}
    echo "✅ Scheduler service account created"
else
    echo "✅ Scheduler service account exists"
fi

echo ""
echo "📋 Step 3: Deploying Cloud Function (without public access)..."

# Deploy Cloud Function WITHOUT --allow-unauthenticated
gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --runtime=python311 \
  --region=${REGION} \
  --source=. \
  --entry-point=descope_sync \
  --trigger-http \
  --no-allow-unauthenticated \
  --timeout=540s \
  --memory=512MB \
  --service-account=${SERVICE_ACCOUNT} \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID}" \
  --project=${PROJECT_ID}

if [ $? -ne 0 ]; then
    echo "❌ Deployment failed!"
    exit 1
fi

echo "✅ Cloud Function deployed successfully!"

# Get the function URL
FUNCTION_URL=$(gcloud functions describe ${FUNCTION_NAME} \
  --gen2 \
  --region=${REGION} \
  --project=${PROJECT_ID} \
  --format='value(serviceConfig.uri)')

echo "📍 Function URL: ${FUNCTION_URL}"

echo ""
echo "📋 Step 4: Granting scheduler service account permission to invoke function..."

# Grant scheduler service account permission to invoke the function
gcloud functions add-iam-policy-binding ${FUNCTION_NAME} \
  --gen2 \
  --region=${REGION} \
  --member="serviceAccount:${SCHEDULER_SA}" \
  --role="roles/run.invoker" \
  --project=${PROJECT_ID}

echo ""
echo "📋 Step 5: Creating/updating Cloud Scheduler job with authentication..."

# Create Cloud Scheduler job with OIDC authentication
gcloud scheduler jobs create http descope-sync-daily \
  --location=${REGION} \
  --schedule="0 2 * * *" \
  --uri="${FUNCTION_URL}" \
  --http-method=POST \
  --time-zone="UTC" \
  --project=${PROJECT_ID} \
  --description="Daily sync of Descope users to BigQuery" \
  --oidc-service-account-email="${SCHEDULER_SA}" \
  --oidc-token-audience="${FUNCTION_URL}" \
  2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Cloud Scheduler job created successfully!"
else
    echo "⚠️  Cloud Scheduler job may already exist. Updating..."
    gcloud scheduler jobs update http descope-sync-daily \
      --location=${REGION} \
      --schedule="0 2 * * *" \
      --uri="${FUNCTION_URL}" \
      --http-method=POST \
      --time-zone="UTC" \
      --oidc-service-account-email="${SCHEDULER_SA}" \
      --oidc-token-audience="${FUNCTION_URL}" \
      --project=${PROJECT_ID}
fi

echo ""
echo "✅✅✅ SECURE DEPLOYMENT COMPLETE! ✅✅✅"
echo ""
echo "🔒 Security Features Enabled:"
echo "  ✅ Secrets stored in Google Secret Manager (not env vars)"
echo "  ✅ Function requires authentication (not public)"
echo "  ✅ Cloud Scheduler uses OIDC authentication"
echo "  ✅ Request validation in function code"
echo "  ✅ Service accounts with least-privilege access"
echo ""
echo "📋 Next steps:"
echo "1. Test the function: gcloud scheduler jobs run descope-sync-daily --location=${REGION} --project=${PROJECT_ID}"
echo "2. View logs: gcloud functions logs read ${FUNCTION_NAME} --gen2 --region=${REGION} --project=${PROJECT_ID}"
echo "3. View scheduler jobs: gcloud scheduler jobs list --project=${PROJECT_ID}"
echo ""
echo "⚠️  NOTE: The function URL is NOT publicly accessible anymore."
echo "   Only Cloud Scheduler can trigger it (authenticated requests only)."

