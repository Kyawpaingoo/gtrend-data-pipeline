FROM python:3.15-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ingestion/ .

# Workload Identity Federation:
# Point to the WIF credential config file mounted via Secret Manager.
# This is NOT a service account key — it's a config that tells the SDK
# how to exchange GitHub OIDC tokens for GCP access tokens.
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/wif-credential-config.json

CMD ["python", "ingest.py"]