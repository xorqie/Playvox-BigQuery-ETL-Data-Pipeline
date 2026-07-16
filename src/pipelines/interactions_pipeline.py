"""
interactions_pipeline.py
--------------------------
Extracts customer service interactions (the raw contact log: calls,
chats, tickets routed through Playvox) and loads a full snapshot into
BigQuery.

Full refresh (not incremental): interaction records carry nested,
variable-shape fields (tags, collaborators) and are the base fact table
that scorecards/evaluations reference, so a full re-pull keeps the table
trivially consistent with Playvox at the cost of a modest daily load - an
acceptable trade at this data volume.
"""

from typing import Any, Dict, List

from config import PlayvoxConfig, BigQueryConfig
from src.playvox_client import PlayvoxClient
from src.bigquery_loader import BigQueryLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)


def transform(raw_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []
    for item in raw_results:
        try:
            records.append({
                "integration_id": item.get("integration_id"),
                "assignee_id": item.get("assignee_id"),
                "collaborators": item.get("collaborators", []),
                "is_deleted": item.get("is_deleted"),
                "tags": item.get("tags", []),
                "created_at": item.get("created_at"),
                "site_id": item.get("site_id"),
                "updated_at": item.get("updated_at"),
                "created_by": item.get("created_by"),
                "priority": item.get("priority"),
                "interaction_date": item.get("interaction_date"),
                "ent_id": item.get("ent_id"),
                "_id": item.get("_id"),
                "csat": item.get("csat"),
                "interaction_id": item.get("interaction_id"),
            })
        except Exception as e:
            logger.error(f"Error transforming interaction record: {e}. Item skipped.")
    return records


def run() -> None:
    playvox_cfg = PlayvoxConfig()
    bq_cfg = BigQueryConfig()

    client = PlayvoxClient(auth=playvox_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info("Fetching interactions from Playvox...")
    raw_results = client.fetch_all_pages(playvox_cfg.endpoint("interactions"))

    if not raw_results:
        logger.info("No interaction data returned from Playvox. Nothing to load.")
        return

    records = transform(raw_results)
    rows_loaded = loader.load_json_truncate(records, bq_cfg.interactions_table)
    logger.info(f"Interactions pipeline complete. {rows_loaded} rows loaded into {bq_cfg.interactions_table}.")


if __name__ == "__main__":
    run()
