"""
BigQuery Loader
Discovers today's CSV partitions in GCS and loads them into BigQuery raw tables.
Designed to be idempotent — re-running will not duplicate data because ingest.py
already writes directly to BQ via load_table_from_dataframe. This script is a
fallback / backfill tool for loading from GCS when direct BQ insertion was skipped.
"""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery, storage

from config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Maps GCS prefix -> BigQuery table name
DATASET_TABLE_MAP = {
    "trending": "trending",
    "interest_over_time": "interest_over_time",
    "related_queries": "related_queries",
}

# Fixed BigQuery schemas — must match the CSV column order written by ingest.py.
#
# trending          : run_ts, keyword, geo, rank
# interest_over_time: run_ts, geo, timestamp, keyword, interest_value   (long form)
# related_queries   : run_ts, geo, keyword, query_type, query, value
SCHEMAS = {
    "trending": [
        bigquery.SchemaField("run_ts", "TIMESTAMP"),
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("geo", "STRING"),
        bigquery.SchemaField("rank", "INTEGER"),
    ],
    "interest_over_time": [
        bigquery.SchemaField("run_ts", "TIMESTAMP"),
        bigquery.SchemaField("geo", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("interest_value", "INTEGER"),
    ],
    "related_queries": [
        bigquery.SchemaField("run_ts", "TIMESTAMP"),
        bigquery.SchemaField("geo", "STRING"),
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("query_type", "STRING"),
        bigquery.SchemaField("query", "STRING"),
        bigquery.SchemaField("value", "STRING"),
    ],
}


def list_blobs_for_date(gcs: storage.Client, bucket_name: str, prefix: str, date_str: str) -> list:
    """Return all GCS blobs under <prefix>/geo=*/date=<date_str>/."""
    # List blobs across all geo partitions for the given date
    blobs = []
    for blob in gcs.list_blobs(bucket_name, prefix=f"{prefix}/"):
        if f"/date={date_str}/" in blob.name and blob.name.endswith(".csv"):
            blobs.append(blob)
    return blobs


def load_blob_to_bq(
    bq: bigquery.Client,
    blob: storage.Blob,
    dataset_id: str,
    table_id: str,
    schema: list,
) -> None:
    """Load a single GCS CSV blob into a BigQuery table (append)."""
    table_ref = f"{bq.project}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=schema,
        skip_leading_rows=1,          # skip the CSV header row
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    uri = f"gs://{blob.bucket.name}/{blob.name}"
    load_job = bq.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    logger.info("Loaded %s -> %s (%d rows)", uri, table_ref, load_job.output_rows)


def ensure_dataset(bq: bigquery.Client, dataset_id: str, location: str = "asia-southeast1") -> None:
    dataset = bigquery.Dataset(f"{bq.project}.{dataset_id}")
    dataset.location = location
    bq.create_dataset(dataset, exists_ok=True)
    logger.info("Dataset ready: %s", dataset_id)


def run() -> None:
    cfg = load_config()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    gcs = storage.Client(project=cfg["project_id"])
    bq = bigquery.Client(project=cfg["project_id"])

    ensure_dataset(bq, cfg["bq_dataset_raw"])

    for gcs_prefix, table_name in DATASET_TABLE_MAP.items():
        blobs = list_blobs_for_date(gcs, cfg["gcs_bucket"], gcs_prefix, today)
        if not blobs:
            logger.info("No blobs for prefix=%s date=%s", gcs_prefix, today)
            continue
        for blob in blobs:
            load_blob_to_bq(bq, blob, cfg["bq_dataset_raw"], table_name, SCHEMAS[table_name])

    logger.info("BQ load complete for date=%s", today)


if __name__ == "__main__":
    run()