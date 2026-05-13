#!/usr/bin/env bash
# Build Docker image, push to Artifact Registry,
# deploy Cloud Run Job, and create Cloud Scheduler trigger.
#
# Usage:
#   export PROJECT_ID=your-project
#   export GCS_BUCKET=your-bucket
#   export REGION=asia-southeast1
#   bash scripts/deploy.sh

set -euo pipefail

: "${PROJECT_ID:?}"
: "${GCS_BUCKET:?}"
REGION="${REGION:-asia-southeast1}"
REPO="gtrends-pipeline"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/ingestor:latest"
JOB_NAME="gtrends-ingestor"
SA_EMAIL="gtrends-pipeline-sa@${PROJECT_ID}.iam.gserviceaccount.com"
SCHEDULE="0 */6 * * *"   # every 6 hours

echo "==> Creating Artifact Registry repository (if not exists)..."
gcloud artifacts repositories create "${REPO}" \
  --repository-format=docker \
  --location="${REGION}" \
  --project="${PROJECT_ID}" || true

echo "==> Authenticating Docker with Artifact Registry..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "==> Building Docker image..."
docker build -t "${IMAGE}" .

echo "==> Pushing image..."
docker push "${IMAGE}"

echo "==> Creating GCS bucket (if not exists)..."
gcloud storage buckets create "gs://${GCS_BUCKET}" \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --uniform-bucket-level-access || true

echo "==> Deploying Cloud Run Job: ${JOB_NAME}..."
gcloud run jobs deploy "${JOB_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --service-account="${SA_EMAIL}" \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},GCS_BUCKET=${GCS_BUCKET},GEO_TARGETS=TH,US,GB,JP,SG" \
  --set-secrets="/secrets/wif-credential-config.json=wif-credential-config:latest" \
  --task-timeout=600 \
  --max-retries=3 \
  --memory=512Mi \
  --cpu=1

echo "==> Creating/updating Cloud Scheduler job..."
gcloud scheduler jobs create http "trigger-${JOB_NAME}" \
  --schedule="${SCHEDULE}" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run" \
  --message-body='{}' \
  --oauth-service-account-email="${SA_EMAIL}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" || \
gcloud scheduler jobs update http "trigger-${JOB_NAME}" \
  --schedule="${SCHEDULE}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}"

echo ""
echo "Deployment complete!"
echo "  Image:    ${IMAGE}"
echo "  Job:      ${JOB_NAME} (${REGION})"
echo "  Schedule: ${SCHEDULE} UTC"
echo ""
echo "To run manually:"
echo "  gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"