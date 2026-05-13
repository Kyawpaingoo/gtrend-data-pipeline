# Google Search Trends Analysis Pipeline

A batch data engineering pipeline on Google Cloud Platform that ingests
real-time Google Search Trends data, stores it in a partitioned data lake,
transforms it via dbt, and visualises insights in Looker Studio.

## Architecture

  [Google Trends (pytrends)]
            |
            v  every 6 hours
  [Cloud Scheduler] --> [Cloud Run Job]
                               |
              +----------------+
              v
  [Cloud Storage (GCS)]              <- Data Lake
  gs://bucket/
    trending/geo=TH/date=2025-05-08/
    interest_over_time/geo=TH/date=2025-05-08/
    related_queries/geo=TH/date=2025-05-08/
              |
              v
  [BigQuery gtrends_raw]             <- Raw warehouse layer
  [BigQuery gtrends_staging]         <- dbt staging views
  [BigQuery gtrends_mart]            <- dbt fact/dim tables
              |
              v
  [Looker Studio Dashboard]          <- Insights

## Security: Workload Identity Federation

No service account key files stored anywhere. GitHub Actions exchanges
its OIDC token for a short-lived GCP access token via the WIF pool.
Cloud Run accesses the WIF credential config through Secret Manager.

## Setup

  1. git clone https://github.com/YOUR_USERNAME/gtrends-pipeline.git
  2. export PROJECT_ID=your-project GITHUB_REPO=owner/repo
  3. bash scripts/setup_wif.sh
  4. Add GitHub Actions secrets (see README)
  5. Push to main — CI/CD handles the rest

## Run locally

  cp ingestion/config.yaml.example ingestion/config.yaml
  # edit config.yaml with your project_id and gcs_bucket
  pip install -r requirements.txt
  python ingestion/ingest.py
  python ingestion/bq_loader.py

## dbt

  cd dbt && pip install dbt-bigquery
  export PROJECT_ID=your-project
  dbt deps && dbt run --profiles-dir . && dbt test --profiles-dir .

## Dashboard

  Link: https://lookerstudio.google.com/your-dashboard-link