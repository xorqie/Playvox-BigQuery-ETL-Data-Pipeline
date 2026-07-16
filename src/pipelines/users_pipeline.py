"""
users_pipeline.py
--------------------
Extracts Playvox users (agents, team leaders, QA analysts) created or
updated in the last N days, and upserts them into BigQuery.

Incremental by design: user profiles change occasionally (role changes,
status updates) but the full roster is stable, so a rolling N-day window
keeps the table current without re-pulling the entire user base on every
run. The `users` table is also a lookup source for the coachings and
evaluations pipelines (agent/coach/evaluator name resolution), so keeping
it fresh matters beyond its own reporting value.
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
                "username": item.get("username"),
                "founder": item.get("founder"),
                "last_name": item.get("last_name"),
                "site_id": item.get("site_id"),
                "updated_at": item.get("updated_at"),
                "created_by": item.get("created_by"),
                "last_login_date": item.get("last_login_date"),
                "type": item.get("type"),
                "email": item.get("email"),
                "ent_id": item.get("ent_id"),
                "tz": item.get("tz"),
                "lang": item.get("lang"),
                "name": item.get("name"),
                "roles": item.get("roles"),
                "light": item.get("light"),
                "created_at": item.get("created_at"),
                "status": item.get("status"),
                # (source API payload had "created_at" listed twice; deduplicated here)
                "_id": item.get("_id"),
            })
        except Exception as e:
            logger.error(f"Error transforming user record: {e}. Item skipped.")
    return pd.DataFrame(rows)


def run() -> None:
    playvox_cfg = PlayvoxConfig()
    bq_cfg = BigQueryConfig()

    client = PlayvoxClient(auth=playvox_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info(f"Fetching users updated in the last {INCREMENTAL_WINDOW_DAYS} days from Playvox...")
    raw_results = client.fetch_all_pages(playvox_cfg.endpoint("users", per_page=10))
    recent = filter_recent(raw_results, days=INCREMENTAL_WINDOW_DAYS)
    logger.info(f"{len(recent)} of {len(raw_results)} fetched users fall within the sync window.")

    if not recent:
        logger.info("No recent user data to load.")
        return

    df = transform(recent)
    rows_loaded = loader.load_merge(df, bq_cfg.users_table, merge_key="_id")
    logger.info(f"Users pipeline complete. {rows_loaded} rows merged into {bq_cfg.users_table}.")


if __name__ == "__main__":
    run()
