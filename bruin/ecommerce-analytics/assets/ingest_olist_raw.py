"""@bruin
name: raw.ingest_olist_raw
type: python
@bruin"""

from __future__ import annotations

import json
import os
import re
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


@dataclass(frozen=True)
class OlistFileSpec:
    filename: str
    table: str


OLIST_FILES: list[OlistFileSpec] = [
    OlistFileSpec("olist_customers_dataset.csv", "customers"),
    OlistFileSpec("olist_geolocation_dataset.csv", "geolocation"),
    OlistFileSpec("olist_order_items_dataset.csv", "order_items"),
    OlistFileSpec("olist_order_payments_dataset.csv", "order_payments"),
    OlistFileSpec("olist_order_reviews_dataset.csv", "order_reviews"),
    OlistFileSpec("olist_orders_dataset.csv", "orders"),
    OlistFileSpec("olist_products_dataset.csv", "products"),
    OlistFileSpec("olist_sellers_dataset.csv", "sellers"),
    OlistFileSpec("product_category_name_translation.csv", "product_category_name_translation"),
]


def _vars() -> dict[str, Any]:
    raw = os.environ.get("BRUIN_VARS") or "{}"
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _safe_ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name


def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def _download_kaggle_dataset(dataset: str, out_dir: Path) -> None:
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi  # type: ignore
    except Exception as e:
        raise RuntimeError("kaggle package is required for downloading.") from e

    if not os.environ.get("KAGGLE_USERNAME") or not os.environ.get("KAGGLE_KEY"):
        raise RuntimeError("Kaggle credentials missing.")

    _ensure_dirs(out_dir)
    api = KaggleApi()
    api.authenticate()

    tmp_dir = out_dir / "_kaggle_tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    api.dataset_download_files(dataset, path=str(tmp_dir), quiet=False, unzip=False)
    zips = list(tmp_dir.glob("*.zip"))
    for z in zips:
        with zipfile.ZipFile(z, "r") as zf:
            zf.extractall(out_dir)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _get_bq_client(project_id: str, key_path: str) -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(key_path)
    return bigquery.Client(credentials=credentials, project=project_id)


def _init_bq_datasets(client: bigquery.Client) -> None:
    for ds_id in ["raw", "staging", "mart", "meta"]:
        dataset = bigquery.Dataset(f"{client.project}.{ds_id}")
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)


def main() -> None:
    vars = _vars()
    raw_dir = Path(vars.get("raw_data_dir") or "data/raw/olist")
    kaggle_dataset = str(vars.get("kaggle_dataset") or "olistbr/brazilian-ecommerce")
    
    # Get credentials from environment or use defaults for local setup
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "your-project-id")
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json")

    # 1. Download data if needed
    missing = [f for f in OLIST_FILES if not (raw_dir / f.filename).exists()]
    if missing:
        _download_kaggle_dataset(kaggle_dataset, raw_dir)

    # 2. Setup BigQuery
    client = _get_bq_client(project_id, key_path)
    _init_bq_datasets(client)

    run_id = f"manual-{datetime.now(timezone.utc).isoformat()}"
    
    # 3. Upload files
    for spec in OLIST_FILES:
        csv_path = raw_dir / spec.filename
        table_id = f"{project_id}.raw.{_safe_ident(spec.table)}"
        
        print(f"[ingest] loading {spec.filename} -> {table_id}")
        
        # Load directly to BigQuery using pandas-gbq for convenience
        df = pd.read_csv(csv_path)
        df.to_gbq(
            destination_table=f"raw.{spec.table}",
            project_id=project_id,
            if_exists='replace',
            credentials=credentials
        )

    print("[ingest] done")


if __name__ == "__main__":
    main()
