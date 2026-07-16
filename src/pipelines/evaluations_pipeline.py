"""
evaluations_pipeline.py
--------------------------
Extracts QA evaluations (a completed scorecard graded against a specific
interaction: section-by-section answers, scores, pass/fail) created in
the last N days, enriches them with human-readable agent/team/evaluator
names, and upserts them into BigQuery.

This is typically the most analytically valuable table in the whole
pipeline - it's what QA trend dashboards, agent scorecards, and coaching
targeting are built on.
"""

import re
from typing import Any, Dict, List, Optional

import pandas as pd

from config import PlayvoxConfig, BigQueryConfig, INCREMENTAL_WINDOW_DAYS
from src.playvox_client import PlayvoxClient
from src.bigquery_loader import BigQueryLoader
from src.utils.filters import filter_recent
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _strip_html(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    return re.sub(r"<[^>]+>", "", text)


def _round_or_none(value):
    if isinstance(value, float):
        return int(round(value))
    return value


def transform(
    records: List[Dict[str, Any]],
    user_names: Dict[str, str],
    team_names: Dict[str, str],
) -> pd.DataFrame:
    rows = []
    for item in records:
        try:
            agent_id = item.get("agent_id")
            team_id = item.get("team_id")
            monitor_id = item.get("monitor_id")

            rows.append({
                "status": item.get("status"),
                "calibration_id": item.get("calibration_id"),
                "agent_name": user_names.get(agent_id, ""),
                "team_name": team_names.get(team_id, ""),
                "evaluator_name": user_names.get(monitor_id, ""),
                "attachments": item.get("attachments") or None,
                "reference": item.get("reference"),
                "sequence": item.get("sequence"),
                "site_id": item.get("site_id"),
                "updated_at": item.get("updated_at"),
                "agent_agree": item.get("agent_agree"),
                "team_id": team_id,
                "agent_id": agent_id,
                "custom_fields": [
                    {"_id": f.get("_id"), "value": f.get("value")}
                    for f in item.get("custom_fields", [])
                ] or None,
                "feedback": _strip_html(item.get("feedback", "")),
                "total_errors": item.get("total_errors"),
                "collaborators": item.get("collaborators") or None,
                "monitor_id": monitor_id,
                "team_leader_id": item.get("team_leader_id"),
                "created_at": item.get("created_at"),
                "scorecard_id": item.get("scorecard_id"),
                "event_subtype": item.get("event_subtype"),
                "comments": item.get("comments") or None,
                "score_avg": _round_or_none(item.get("score_avg")),
                "score": _round_or_none(item.get("score")),
                "passed": item.get("passed"),
                "event_type": item.get("event_type"),
                "date_created": item.get("date_created"),
                "_id": item.get("_id"),
                "sections": [
                    {
                        "comment": section.get("comment"),
                        "type_fail": section.get("type_fail"),
                        "answers": [
                            {
                                "comment": answer.get("comment"),
                                "answer_id": answer.get("answer_id"),
                                "value": answer.get("value"),
                                "answer_type": answer.get("answer_type"),
                                "question_id": answer.get("question_id"),
                            }
                            for answer in section.get("answers", [])
                        ] or None,
                        "score": _round_or_none(section.get("score")),
                        "passed": section.get("passed"),
                        "_id": section.get("_id"),
                        "custom_fields": section.get("custom_fields") or None,
                    }
                    for section in item.get("sections", [])
                ] or None,
                "coaching": [
                    {
                        "coaching_id": c.get("coaching_id"),
                        "coaching_sequence": c.get("coaching_sequence"),
                    }
                    for c in item.get("coaching", [])
                ] or None,
                "interaction_id": item.get("interaction_id"),
            })
        except Exception as e:
            logger.error(f"Error transforming evaluation record: {e}. Item skipped.")
    return pd.DataFrame(rows)


def run() -> None:
    playvox_cfg = PlayvoxConfig()
    bq_cfg = BigQueryConfig()

    client = PlayvoxClient(auth=playvox_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info(f"Fetching evaluations created in the last {INCREMENTAL_WINDOW_DAYS} days from Playvox...")
    raw_results = client.fetch_all_pages(playvox_cfg.endpoint("evaluations"))
    recent = filter_recent(raw_results, days=INCREMENTAL_WINDOW_DAYS, date_fields=("created_at",))
    logger.info(f"{len(recent)} of {len(raw_results)} fetched evaluations fall within the sync window.")

    if not recent:
        logger.info("No recent evaluation data to load.")
        return

    logger.info("Building name lookups from the users and teams tables...")
    user_names = loader.fetch_lookup_map(bq_cfg.users_table, "_id", "CONCAT(name, ' ', last_name)")
    team_names = loader.fetch_lookup_map(bq_cfg.teams_table, "_id", "name")

    df = transform(recent, user_names, team_names)
    rows_loaded = loader.load_merge(df, bq_cfg.evaluations_table, merge_key="_id")
    logger.info(f"Evaluations pipeline complete. {rows_loaded} rows merged into {bq_cfg.evaluations_table}.")


if __name__ == "__main__":
    run()
