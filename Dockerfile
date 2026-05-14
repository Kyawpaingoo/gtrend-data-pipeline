FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ .

# Cloud Run resolves credentials automatically via the metadata server (ADC)
# using the service account attached to the job — no credential file needed here.
# GOOGLE_APPLICATION_CREDENTIALS is only required for external workloads (e.g. local dev,
# GitHub Actions) that use WIF to impersonate a GCP service account.

CMD ["python", "ingest.py"]