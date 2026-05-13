#!/usr/bin/env bash
# Workload Identity Federation setup for Google Search Trends Pipeline
#
# What this does:
#   1. Creates a Workload Identity Pool
#   2. Creates a GitHub Actions OIDC provider inside the pool
#   3. Creates a Service Account with least-privilege roles
#   4. Binds the SA to the WIF pool (no key file ever generated)
#   5. Generates a credential config JSON → stored in Secret Manager
#
# Usage:
#   export PROJECT_ID=your-gcp-project-id
#   export GITHUB_REPO=your-username/your-repo-name
#   bash scripts/setup_wif.sh

set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID environment variable}"
: "${GITHUB_REPO:?Set GITHUB_REPO as owner/repo e.g. kevin/gtrends-pipeline}"

POOL_ID="gtrends-wif-pool"
PROVIDER_ID="github-actions-provider"
SA_NAME="gtrends-pipeline-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
LOCATION="global"

echo "==> Enabling required GCP APIs..."
gcloud services enable \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  cloudresourcemanager.googleapis.com \
  storage.googleapis.com \
  bigquery.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  --project="${PROJECT_ID}"

echo "==> Creating Workload Identity Pool: ${POOL_ID}"
gcloud iam workload-identity-pools create "${POOL_ID}" \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}" \
  --display-name="Google Trends Pipeline WIF Pool" \
  --description="Allows GitHub Actions to authenticate without service account keys" || true

echo "==> Creating OIDC Provider: ${PROVIDER_ID}"
gcloud iam workload-identity-pools providers create-oidc "${PROVIDER_ID}" \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}" \
  --workload-identity-pool="${POOL_ID}" \
  --display-name="GitHub Actions OIDC" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.actor=assertion.actor" \
  --attribute-condition="assertion.repository == '${GITHUB_REPO}'" || true

echo "==> Creating Service Account: ${SA_EMAIL}"
gcloud iam service-accounts create "${SA_NAME}" \
  --project="${PROJECT_ID}" \
  --display-name="Google Trends Pipeline SA" \
  --description="Least-privilege SA for gtrends pipeline — no key file, WIF only" || true

echo "==> Granting least-privilege IAM roles..."
for ROLE in \
  "roles/storage.objectAdmin" \
  "roles/bigquery.dataEditor" \
  "roles/bigquery.jobUser" \
  "roles/secretmanager.secretAccessor" \
  "roles/run.invoker"; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${ROLE}" \
    --condition=None
done

echo "==> Binding WIF Pool to Service Account..."
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')
POOL_RESOURCE="projects/${PROJECT_NUMBER}/locations/${LOCATION}/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}"

gcloud iam service-accounts add-iam-policy-binding "${SA_EMAIL}" \
  --project="${PROJECT_ID}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${POOL_RESOURCE%/providers/*}/attribute.repository/${GITHUB_REPO}"

echo "==> Generating WIF credential config (NOT a key file)..."
gcloud iam workload-identity-pools create-cred-config \
  "projects/${PROJECT_NUMBER}/locations/${LOCATION}/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}" \
  --service-account="${SA_EMAIL}" \
  --output-file="wif-credential-config.json" \
  --credential-source-file=/var/run/secrets/token \
  --credential-source-type=text

echo "==> Storing credential config in Secret Manager..."
gcloud secrets create wif-credential-config \
  --data-file=wif-credential-config.json \
  --project="${PROJECT_ID}" || \
gcloud secrets versions add wif-credential-config \
  --data-file=wif-credential-config.json \
  --project="${PROJECT_ID}"

rm wif-credential-config.json
echo "Local credential config deleted — stored safely in Secret Manager only."

echo ""
echo "WIF setup complete!"
echo ""
echo "Add these GitHub Actions secrets:"
echo "  WIF_PROVIDER  = projects/${PROJECT_NUMBER}/locations/${LOCATION}/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}"
echo "  WIF_SA_EMAIL  = ${SA_EMAIL}"
echo "  PROJECT_ID    = ${PROJECT_ID}"
echo "  GCS_BUCKET    = gtrends-data-lake"
echo "  REGION        = asia-southeast1"