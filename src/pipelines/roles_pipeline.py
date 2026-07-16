"""
roles_pipeline.py
--------------------
Extracts Playvox permission roles (what a "QA Analyst", "Team Leader",
etc. is allowed to see/do) created or updated in the last N days, and
upserts them into BigQuery.

Roles change rarely - this pipeline exists mainly for completeness/audit
purposes (e.g. answering "what permissions did this role have on date X"
alongside the other tables), so it follows the same incremental pattern
as users/teams for consistency even though its own data volume is tiny.
"""

from typing import Any, Dict, List

import pandas as pd

from config import PlayvoxConfig, BigQueryConfig, INCREMENTAL_WINDOW_DAYS
from src.playvox_client import PlayvoxClient
from src.bigquery_loader import BigQueryLoader
from src.utils.filters import filter_recent
from src.utils.logger import get_logger

logger = get_logger(__name__)


def transform(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in records:
        try:
            rows.append({
                "code": item.get("code"),
                "role_inherit": item.get("role_inherit"),
                "name": item.get("name"),
                "is_default": bool(item.get("default")),  # "default" is a reserved word in SQL; renamed for the warehouse
                "created_at_readable": item.get("created_at_readable"),
                "created_at": item.get("created_at"),
                "site_id": item.get("site_id"),
                "updated_at": item.get("updated_at"),
                "updated_at_readable": item.get("updated_at_readable"),
                "write": item.get("write", []),
                "read": item.get("read", []),
                "_id": item.get("_id"),
                "description": item.get("description"),
            })
        except Exception as e:
            logger.error(f"Error transforming role record: {e}. Item skipped.")
    return pd.DataFrame(rows)


def run() -> None:
    playvox_cfg = PlayvoxConfig()
    bq_cfg = BigQueryConfig()

    client = PlayvoxClient(auth=playvox_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info(f"Fetching roles created/updated in the last {INCREMENTAL_WINDOW_DAYS} days from Playvox...")
    raw_results = client.fetch_all_pages(playvox_cfg.endpoint("roles", per_page=10))
    recent = filter_recent(raw_results, days=INCREMENTAL_WINDOW_DAYS)
    logger.info(f"{len(recent)} of {len(raw_results)} fetched roles fall within the sync window.")

    if not recent:
        logger.info("No recent role data to load.")
        return

    df = transform(recent)
    rows_loaded = loader.load_merge(df, bq_cfg.roles_table, merge_key="_id")
    logger.info(f"Roles pipeline complete. {rows_loaded} rows merged into {bq_cfg.roles_table}.")


if __name__ == "__main__":
    run()
