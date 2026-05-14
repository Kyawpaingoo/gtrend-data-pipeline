FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ .

# WIF credential config is mounted via --set-secrets in cloudbuild.yaml
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/wif-credential-config.json

CMD ["python", "ingest.py"]