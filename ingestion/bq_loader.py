"""
BigQuery Loader
Discovers new partitions in GCS and loads them into BigQuery raw dataset.
Designed to be idempotent — re-running will not duplicate data.
"""

import logging
import os
from datetime import datetime, timezone

from google.cloud import bigquery, storage

from config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DATASET_TABLE_MAP = {
    "trending": "raw_trending_searches",
    "interest_over_time": "raw_interest_over_time",
    "related_queries": "raw_related_queries",
}

SCHEMAS = {
    "raw_trending_searches": [
        bigquery.SchemaField("run_ts", "TIMESTAMP"),
        bigquery.SchemaField("geo", "STRING"),
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("rank", "INTEGER"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ],
    "raw_interest_over_time": [
        bigquery.SchemaField("run_ts", "TIMESTAMP"),
        bigquery.SchemaField("geo", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("interest_value", "INTEGER"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ],
    "raw_related_queries": [
        bigquery.SchemaField("run_ts", "TIMESTAMP"),
        bigquery.SchemaField("geo", "STRING"),
        bigquery.SchemaField("keyword", "STRING"),
        bigquery.SchemaField("query_type", "STRING"),
        bigquery.SchemaField("related_query", "STRING"),
        bigquery.SchemaField("value", "STRING"),
        bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
    ],
}


def list_new_blobs(gcs, bucket_name, prefix, date_str):
    full_prefix = f"{prefix}/date={date_str}/"
    return list(gcs.list_blobs(bucket_name, prefix=full_prefix))


def load_blob_to_bq(bq, blob, dataset_id, table_id, schema):
    table_ref = f"{bq.project}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    uri = f"gs://{blob.bucket.name}/{blob.name}"
    load_job = bq.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    logger.info("Loaded %s -> %s (%d rows)", uri, table_ref, load_job.output_rows)


def ensure_dataset(bq, dataset_id, location="US"):
    dataset = bigquery.Dataset(f"{bq.project}.{dataset_id}")
    dataset.location = location
    bq.create_dataset(dataset, exists_ok=True)
    logger.info("Dataset ready: %s", dataset_id)


def run():
    cfg = load_config()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    gcs = storage.Client(project=cfg["project_id"])
    bq = bigquery.Client(project=cfg["project_id"])

    ensure_dataset(bq, cfg["bq_dataset_raw"])

    for gcs_prefix, table_name in DATASET_TABLE_MAP.items():
        blobs = list_new_blobs(gcs, cfg["gcs_bucket"], gcs_prefix, today)
        if not blobs:
            logger.info("No new blobs for prefix=%s date=%s", gcs_prefix, today)
            continue
        for blob in blobs:
            load_blob_to_bq(bq, blob, cfg["bq_dataset_raw"], table_name, SCHEMAS[table_name])

    logger.info("BQ load complete for date=%s", today)


if __name__ == "__main__":
    run()