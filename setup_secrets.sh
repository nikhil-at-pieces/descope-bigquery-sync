#!/bin/bash
# Store Descope credentials in Google Secret Manager (no secrets in code).
# Run once per GCP project. Use env vars or prompts (nothing committed).

set -e

PROJECT_ID="${GCP_PROJECT:-global-cloud-runtime}"
SERVICE_ACCOUNT="descope-sync-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🔒 Setting up Google Secret Manager for Descope credentials..."
echo "   Project: ${PROJECT_ID}"
echo ""

# Enable Secret Manager API
echo "📦 Enabling Secret Manager API..."
gcloud services enable secretmanager.googleapis.com --project="${PROJECT_ID}"

# Get values from env or prompt (never commit these)
if [ -n "${DESCOPE_PROJECT_ID}" ] && [ -n "${DESCOPE_MANAGEMENT_KEY}" ]; then
    echo "   Using DESCOPE_PROJECT_ID and DESCOPE_MANAGEMENT_KEY from environment."
    PROJECT_ID_VAL="${DESCOPE_PROJECT_ID}"
    MANAGEMENT_KEY_VAL="${DESCOPE_MANAGEMENT_KEY}"
else
    echo "   Enter Descope Project ID (from Descope console):"
    read -r PROJECT_ID_VAL
    echo "   Enter Descope Management Key (from Descope console):"
    read -rs MANAGEMENT_KEY_VAL
    echo ""
fi

if [ -z "${PROJECT_ID_VAL}" ] || [ -z "${MANAGEMENT_KEY_VAL}" ]; then
    echo "❌ Both Descope Project ID and Management Key are required."
    exit 1
fi

# Create or update secrets
echo "🔐 Creating/updating secrets..."

for name in descope-project-id descope-management-key; do
    if [ "$name" = "descope-project-id" ]; then
        val="${PROJECT_ID_VAL}"
    else
        val="${MANAGEMENT_KEY_VAL}"
    fi
    if gcloud secrets describe "$name" --project="${PROJECT_ID}" &>/dev/null; then
        echo -n "${val}" | gcloud secrets versions add "$name" --data-file=- --project="${PROJECT_ID}"
        echo "✅ $name updated"
    else
        echo -n "${val}" | gcloud secrets create "$name" \
            --data-file=- \
            --replication-policy="automatic" \
            --project="${PROJECT_ID}"
        echo "✅ $name created"
    fi
done

# Grant function service account access (may not exist yet; run deploy_secure.sh after)
echo ""
echo "🔑 Granting service account access to secrets (if SA exists)..."
gcloud secrets add-iam-policy-binding descope-project-id \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor" \
    --project="${PROJECT_ID}" 2>/dev/null || true

gcloud secrets add-iam-policy-binding descope-management-key \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/secretmanager.secretAccessor" \
    --project="${PROJECT_ID}" 2>/dev/null || true

echo ""
echo "✅ Secrets configured. Run ./deploy_secure.sh to deploy the function."
