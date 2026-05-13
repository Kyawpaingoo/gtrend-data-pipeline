"""
Configuration loader.
Values are read from config.yaml first, then overridden by environment variables.
"""

import os
import yaml
from pathlib import Path

_DEFAULTS = {
    "project_id": "",
    "gcs_bucket": "",
    "geo_targets": ["TH", "US", "GB", "JP", "SG"],
    "timeframe": "now 7-d",
    "request_delay_seconds": 2,
    "bq_dataset_raw": "gtrends_raw",
    "bq_dataset_transformed": "gtrends_mart",
}


def load_config(path: str = "config.yaml") -> dict:
    cfg = dict(_DEFAULTS)

    yaml_path = Path(__file__).parent / path
    if yaml_path.exists():
        with open(yaml_path) as f:
            file_cfg = yaml.safe_load(f) or {}
        cfg.update({k: v for k, v in file_cfg.items() if v is not None})

    env_map = {
        "PROJECT_ID": "project_id",
        "GCS_BUCKET": "gcs_bucket",
        "GEO_TARGETS": "geo_targets",
        "TIMEFRAME": "timeframe",
        "REQUEST_DELAY": "request_delay_seconds",
        "BQ_DATASET_RAW": "bq_dataset_raw",
        "BQ_DATASET_TRANSFORMED": "bq_dataset_transformed",
    }
    for env_key, cfg_key in env_map.items():
        val = os.environ.get(env_key)
        if val is not None:
            if cfg_key == "geo_targets":
                cfg[cfg_key] = [g.strip() for g in val.split(",")]
            elif cfg_key in ("request_delay_seconds",):
                cfg[cfg_key] = float(val)
            else:
                cfg[cfg_key] = val

    _validate(cfg)
    return cfg


def _validate(cfg: dict) -> None:
    missing = [k for k in ("project_id", "gcs_bucket") if not cfg.get(k)]
    if missing:
        raise EnvironmentError(
            f"Missing required config keys: {missing}. "
            "Set them in config.yaml or as environment variables."
        )