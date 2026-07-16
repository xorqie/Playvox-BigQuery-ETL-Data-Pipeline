"""
calibrations_pipeline.py
--------------------------
Extracts calibration sessions - the process where multiple QA analysts
independently score the same interaction to check inter-rater agreement -
and loads a full snapshot into BigQuery.

Full refresh: like scorecards, calibration records are nested (team info,
agent info, per-analyst results) and relatively low-volume, so a full
JSON snapshot is simpler and just as correct as an incremental merge here.
"""

from typing import Any, Dict, List

from config import PlayvoxConfig, BigQueryConfig
from src.playvox_client import PlayvoxClient
from src.bigquery_loader import BigQueryLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _get(item: dict, field: str, default=None):
    return item.get(field, default) if item else default


def transform(raw_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []
    for item in raw_results:
        try:
            team_info = _get(item, "team_info", {}) or {}
            agent_info = _get(item, "agent_info", {}) or {}
            expert = _get(item, "expert", {}) or {}
            expert_info = _get(expert, "expert_info", {}) or {}
            scorecard_info = _get(item, "scorecard_info", {}) or {}
            summary = _get(item, "summary", {}) or {}
            answers_summary = _get(summary, "answers", {}) or {}

            records.append({
                "status": _get(item, "status"),
                "due_date": _get(item, "due_date"),
                "created_at_readable": _get(item, "created_at_readable"),
                "sequence": _get(item, "sequence"),
                "scorecard_id": _get(item, "scorecard_id"),
                "site_id": _get(item, "site_id"),
                "updated_at": _get(item, "updated_at"),
                "completed_at": _get(item, "completed_at"),
                "team_id": _get(item, "team_id"),
                "agent_id": _get(item, "agent_id"),
                "team_info": {
                    "status": team_info.get("status"),
                    "_id": team_info.get("_id"),
                    "name": team_info.get("name"),
                },
                "agent_info": {
                    "last_name": agent_info.get("last_name"),
                    "status": agent_info.get("status"),
                    "_id": agent_info.get("_id"),
                    "email": agent_info.get("email"),
                    "name": agent_info.get("name"),
                },
                "interaction_reference": _get(item, "interaction_reference"),
                "expert": {
                    "_id": expert.get("_id"),
                    "expert_info": {
                        "last_name": expert_info.get("last_name"),
                        "status": expert_info.get("status"),
                        "_id": expert_info.get("_id"),
                        "email": expert_info.get("email"),
                        "name": expert_info.get("name"),
                    },
                },
                "scorecard_info": {
                    "effectiveness_goal": scorecard_info.get("effectiveness_goal"),
                    "_id": scorecard_info.get("_id"),
                    "max_score": scorecard_info.get("max_score"),
                    "target": scorecard_info.get("target"),
                    "name": scorecard_info.get("name"),
                },
                "created_at": _get(item, "created_at"),
                "created_by": _get(item, "created_by"),
                "scorecard_answer_id_reference": _get(item, "scorecard_answer_id_reference"),
                "updated_at_readable": _get(item, "updated_at_readable"),
                "comments": _get(item, "comments"),
                "summary": {
                    "answers": {
                        "total": answers_summary.get("total"),
                        "score": answers_summary.get("score"),
                    }
                },
                "validate_fail_answers": _get(item, "validate_fail_answers"),
                "analysts": [
                    {
                        "analyst_info": {
                            "last_name": (_get(a, "analyst_info", {}) or {}).get("last_name"),
                            "status": (_get(a, "analyst_info", {}) or {}).get("status"),
                            "_id": (_get(a, "analyst_info", {}) or {}).get("_id"),
                            "email": (_get(a, "analyst_info", {}) or {}).get("email"),
                            "name": (_get(a, "analyst_info", {}) or {}).get("name"),
                        },
                        "_id": _get(a, "_id"),
                        "results": {
                            "answers": {
                                "score": (_get(_get(a, "results", {}) or {}, "answers", {}) or {}).get("score"),
                                "equals": (_get(_get(a, "results", {}) or {}, "answers", {}) or {}).get("equals"),
                            }
                        },
                    }
                    for a in _get(item, "analysts", [])
                ],
                "_id": _get(item, "_id"),
                "interaction_id": _get(item, "interaction_id"),
            })
        except Exception as e:
            logger.error(f"Error transforming calibration record: {e}. Item skipped.")
    return records


def run() -> None:
    playvox_cfg = PlayvoxConfig()
    bq_cfg = BigQueryConfig()

    client = PlayvoxClient(auth=playvox_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info("Fetching calibrations from Playvox...")
    raw_results = client.fetch_all_pages(playvox_cfg.endpoint("calibrations"))

    if not raw_results:
        logger.info("No calibration data returned from Playvox. Nothing to load.")
        return

    records = transform(raw_results)
    rows_loaded = loader.load_json_truncate(records, bq_cfg.calibrations_table)
    logger.info(f"Calibrations pipeline complete. {rows_loaded} rows loaded into {bq_cfg.calibrations_table}.")


if __name__ == "__main__":
    run()
