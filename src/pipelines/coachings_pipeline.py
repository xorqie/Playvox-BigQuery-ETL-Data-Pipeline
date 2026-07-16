"""
coachings_pipeline.py
------------------------
Extracts coaching sessions (a team leader/coach working 1:1 with an agent
on a specific improvement area, often triggered by a low QA score) created
or updated in the last N days, enriches them with human-readable agent/
coach/team names, and upserts them into BigQuery.

Enrichment note: the original script pulled the *entire* users and teams
tables out of BigQuery into a throwaway local SQLite database just to do
ID -> name lookups. At this data volume a plain Python dict is simpler,
faster, and removes a dependency entirely - see `BigQueryLoader.fetch_lookup_map`.
"""

import json
from typing import Any, Dict, List

import pandas as pd

from config import PlayvoxConfig, BigQueryConfig, INCREMENTAL_WINDOW_DAYS
from src.playvox_client import PlayvoxClient
from src.bigquery_loader import BigQueryLoader
from src.utils.filters import filter_recent
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _clean_json_braces(value) -> str:
    """Match the original output format: JSON-ish text with brackets/braces stripped for readability."""
    if not value:
        return ""
    return json.dumps(value).replace("[", "").replace("]", "").replace("{", "").replace("}", "")


def _names_for_ids(ids: List[str], lookup: Dict[str, str]) -> List[str]:
    return [lookup[i] for i in ids if i in lookup]


def transform(
    records: List[Dict[str, Any]],
    user_names: Dict[str, str],
    team_names: Dict[str, str],
) -> pd.DataFrame:
    rows = []
    for item in records:
        try:
            team_ids = item.get("team_ids", []) or []
            coach_ids = item.get("coach_id", [])
            coach_ids = coach_ids if isinstance(coach_ids, list) else [coach_ids]
            collaborator_ids = item.get("collaborator_ids", []) or []
            trainee_id = item.get("trainee_id")

            feedback = item.get("feedback", "") or ""
            feedback_clean = " ".join(line.strip() for line in feedback.splitlines() if line.strip())

            rows.append({
                "_id": item.get("_id"),
                "status": item.get("status"),
                "entity_id": item.get("entity_id"),
                "team_ids": ",".join(str(t) for t in team_ids),
                "team_name": ",".join(_names_for_ids(team_ids, team_names)),
                "feedback": feedback_clean,
                "coach_id": ",".join(str(c) for c in coach_ids),
                "coach_name": ",".join(_names_for_ids(coach_ids, user_names)),
                "trainee_id": trainee_id,
                "trainee_name": user_names.get(trainee_id),
                "achieved": item.get("achieved"),
                "trainee_log": _clean_json_braces(item.get("trainee_log")),
                "ent_id": item.get("ent_id"),
                "site_id": item.get("site_id"),
                "updated_at": item.get("updated_at"),
                "satisfaction": str(item.get("satisfaction", "")),
                "seen_coach": item.get("seen_coach"),
                "seen_trainee": item.get("seen_trainee"),
                "kpis_threshold_value": float(item.get("kpis_threshold_value") or 0),
                "kpis_id": item.get("kpis_id"),
                "attachments": ", ".join(str(a) for a in item.get("attachments", [])),
                "follow_up": item.get("follow_up"),
                "stage_id": item.get("stage_id"),
                "entity_type": item.get("entity_type"),
                "data_after_start": item.get("data_after_start"),
                "data_after_end": item.get("data_after_end"),
                "created_at": item.get("created_at"),
                "messages_read": item.get("messages_read"),
                "messages_total": item.get("messages_total"),
                "collaborator_ids": ",".join(str(c) for c in collaborator_ids),
                "data_before_start": item.get("data_before_start"),
                "data_before_end": item.get("data_before_end"),
                "snapshot": _clean_json_braces(item.get("snapshot")),
                "coach_log_total_opens": item.get("coach_log_total_opens"),
                "coach_log_total_followups": item.get("coach_log_total_followups"),
                "coach_log_last_followup": item.get("coach_log_last_followup"),
                "coach_log_interactions": _clean_json_braces(item.get("coach_log_interactions")),
                "custom_area_id": item.get("custom_area_id"),
                "trainee_agree_date": item.get("trainee_agree_date"),
                "trainee_agree_signed": item.get("trainee_agree_signed"),
                "sequence": item.get("sequence"),
                "scorecard_info_id": item.get("scorecard_info_id"),
                "scorecard_info_status": item.get("scorecard_info_status"),
                "scorecard_info_name": item.get("scorecard_info_name"),
                "evaluation_ids": ",".join(str(e) for e in item.get("evaluation_ids", [])),
                "success": item.get("success"),
            })
        except Exception as e:
            logger.error(f"Error transforming coaching record: {e}. Item skipped.")
    return pd.DataFrame(rows)


def run() -> None:
    playvox_cfg = PlayvoxConfig()
    bq_cfg = BigQueryConfig()

    client = PlayvoxClient(auth=playvox_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info(f"Fetching coachings created/updated in the last {INCREMENTAL_WINDOW_DAYS} days from Playvox...")
    raw_results = client.fetch_all_pages(playvox_cfg.endpoint("coachings", per_page=10))
    recent = filter_recent(raw_results, days=INCREMENTAL_WINDOW_DAYS)
    logger.info(f"{len(recent)} of {len(raw_results)} fetched coachings fall within the sync window.")

    if not recent:
        logger.info("No recent coaching data to load.")
        return

    logger.info("Building name lookups from the users and teams tables...")
    user_names = loader.fetch_lookup_map(bq_cfg.users_table, "_id", "CONCAT(name, ' ', last_name)")
    team_names = loader.fetch_lookup_map(bq_cfg.teams_table, "_id", "name")

    df = transform(recent, user_names, team_names)
    rows_loaded = loader.load_merge(df, bq_cfg.coachings_table, merge_key="_id")
    logger.info(f"Coachings pipeline complete. {rows_loaded} rows merged into {bq_cfg.coachings_table}.")


if __name__ == "__main__":
    run()
