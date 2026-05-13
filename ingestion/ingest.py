"""
Google Trends Ingestion Script
Fetches trending topics via pytrends and uploads partitioned JSON to GCS.
Uses Workload Identity Federation — no service account key files needed.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
from google.cloud import storage
from pytrends.request import TrendReq

from config import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def build_pytrends_client() -> TrendReq:
    """Initialise pytrends with retries and a realistic request delay."""
    return TrendReq(hl="en-US", tz=360, timeout=(10, 25), retries=3, backoff_factor=0.5)


def fetch_trending_searches(pt: TrendReq, geo: str) -> pd.DataFrame:
    """Return today's real-time trending searches for a given country code."""
    logger.info("Fetching trending searches for geo=%s", geo)
    df = pt.trending_searches(pn=geo)
    df.columns = ["keyword"]
    df["geo"] = geo
    df["rank"] = range(1, len(df) + 1)
    return df


def fetch_interest_over_time(pt: TrendReq, keywords: list[str], timeframe: str) -> pd.DataFrame:
    """Return interest-over-time scores for up to 5 keywords."""
    logger.info("Fetching interest over time: keywords=%s timeframe=%s", keywords, timeframe)
    pt.build_payload(keywords, timeframe=timeframe)
    df = pt.interest_over_time()
    if df.empty:
        logger.warning("No interest-over-time data returned.")
        return pd.DataFrame()
    df = df.drop(columns=["isPartial"], errors="ignore")
    df = df.reset_index().rename(columns={"date": "timestamp"})
    df["timestamp"] = df["timestamp"].astype(str)
    return df


def fetch_interest_by_region(pt: TrendReq, keywords: list[str]) -> pd.DataFrame:
    """Return interest-by-region breakdown for keywords."""
    logger.info("Fetching interest by region: keywords=%s", keywords)
    pt.build_payload(keywords, timeframe="now 7-d")
    df = pt.interest_by_region(resolution="COUNTRY", inc_low_vol=True, inc_geo_code=True)
    if df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    return df


def fetch_related_queries(pt: TrendReq, keyword: str) -> dict:
    """Return top and rising related queries for a single keyword."""
    logger.info("Fetching related queries for keyword=%s", keyword)
    pt.build_payload([keyword], timeframe="now 7-d")
    related = pt.related_queries()
    result = {}
    for query_type in ("top", "rising"):
        df = related.get(keyword, {}).get(query_type)
        if df is not None and not df.empty:
            result[query_type] = df.to_dict(orient="records")
    return result


def upload_to_gcs(client: storage.Client, bucket_name: str, blob_path: str, payload: dict) -> None:
    """Serialize payload as JSON and upload to GCS."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        data=json.dumps(payload, ensure_ascii=False, default=str),
        content_type="application/json",
    )
    logger.info("Uploaded gs://%s/%s", bucket_name, blob_path)


def build_blob_path(dataset: str, geo: str, run_ts: str) -> str:
    """
    Hive-style partition path for easy BigQuery external table discovery.
    e.g. trending/geo=TH/date=2025-05-08/run_2025-05-08T06:00:00Z.json
    """
    date_part = run_ts[:10]
    return f"{dataset}/geo={geo}/date={date_part}/run_{run_ts}.json"


def run() -> None:
    cfg = load_config()
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # GCS client picks up WIF credentials automatically
    # via GOOGLE_APPLICATION_CREDENTIALS pointing to the WIF credential config file.
    gcs = storage.Client(project=cfg["project_id"])
    pt = build_pytrends_client()

    for geo in cfg["geo_targets"]:
        # 1. Trending searches
        trending_df = fetch_trending_searches(pt, geo)
        top_keywords = trending_df["keyword"].head(5).tolist()
        time.sleep(cfg["request_delay_seconds"])

        upload_to_gcs(
            gcs,
            cfg["gcs_bucket"],
            build_blob_path("trending", geo, run_ts),
            {
                "run_ts": run_ts,
                "geo": geo,
                "rows": trending_df.to_dict(orient="records"),
            },
        )

        # 2. Interest over time for top 5 trending keywords
        iot_df = fetch_interest_over_time(pt, top_keywords, cfg["timeframe"])
        time.sleep(cfg["request_delay_seconds"])

        if not iot_df.empty:
            upload_to_gcs(
                gcs,
                cfg["gcs_bucket"],
                build_blob_path("interest_over_time", geo, run_ts),
                {
                    "run_ts": run_ts,
                    "geo": geo,
                    "keywords": top_keywords,
                    "rows": iot_df.to_dict(orient="records"),
                },
            )

        # 3. Related queries for the #1 trending keyword
        if top_keywords:
            related = fetch_related_queries(pt, top_keywords[0])
            time.sleep(cfg["request_delay_seconds"])
            upload_to_gcs(
                gcs,
                cfg["gcs_bucket"],
                build_blob_path("related_queries", geo, run_ts),
                {
                    "run_ts": run_ts,
                    "geo": geo,
                    "keyword": top_keywords[0],
                    "related": related,
                },
            )

    logger.info("Ingestion complete for run_ts=%s", run_ts)


if __name__ == "__main__":
    run()