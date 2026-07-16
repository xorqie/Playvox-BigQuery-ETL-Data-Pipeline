"""
teams_pipeline.py
--------------------
Extracts Playvox teams (the org units agents and team leaders belong to)
updated in the last N days, and upserts them into BigQuery.

Note on a bug fixed here: the original script fetched pages *concurrently*
with `asyncio`/`aiohttp` but capped itself at a hardcoded `max_pages=5`
regardless of how many pages actually existed - silently dropping any
team created past the 500th record, with no error or warning. Async
fetching also doesn't fit this API well, since Playvox's own rate limit
is a single shared budget - concurrent requests just hit it faster. This
version uses the same sequential, fully-paginated `PlayvoxClient` as every
other pipeline, which is simpler and, unlike the original, complete.
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
                "status": item.get("status"),
                "created_at_readable": item.get("created_at_readable"),
                "name": item.get("name"),
                "default": item.get("default"),
                "team_leader_id": item.get("team_leader_id", []),
                "created_at": item.get("created_at"),
                "site_id": item.get("site_id"),
                "updated_at": item.get("updated_at"),
                "created_by": item.get("created_by"),
                "updated_at_readable": item.get("updated_at_readable"),
                "total_users": item.get("total_users"),
                "team_leader_info": [
                    {
                        "last_name": leader.get("last_name"),
                        "status": leader.get("status"),
                        "_id": leader.get("_id"),
                        "email": leader.get("email"),
                        "name": leader.get("name"),
                    }
                    for leader in item.get("team_leader_info", [])
                ],
                "users_info": [
                    {
                        "last_name": user.get("last_name"),
                        "status": user.get("status"),
                        "_id": user.get("_id"),
                        "email": user.get("email"),
                        "name": user.get("name"),
                    }
                    for user in item.get("users_info", [])
                ],
                "users": item.get("users", []),
                "_id": item.get("_id"),
                "description": item.get("description"),
            })
        except Exception as e:
            logger.error(f"Error transforming team record: {e}. Item skipped.")
    return pd.DataFrame(rows)


def run() -> None:
    playvox_cfg = PlayvoxConfig()
    bq_cfg = BigQueryConfig()

    client = PlayvoxClient(auth=playvox_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info(f"Fetching teams updated in the last {INCREMENTAL_WINDOW_DAYS} days from Playvox...")
    raw_results = client.fetch_all_pages(playvox_cfg.endpoint("teams"))
    recent = filter_recent(raw_results, days=INCREMENTAL_WINDOW_DAYS, date_fields=("updated_at",))
    logger.info(f"{len(recent)} of {len(raw_results)} fetched teams fall within the sync window.")

    if not recent:
        logger.info("No recent team data to load.")
        return

    df = transform(recent)
    rows_loaded = loader.load_merge(df, bq_cfg.teams_table, merge_key="_id")
    logger.info(f"Teams pipeline complete. {rows_loaded} rows merged into {bq_cfg.teams_table}.")


if __name__ == "__main__":
    run()
