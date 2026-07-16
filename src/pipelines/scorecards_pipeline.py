"""
scorecards_pipeline.py
------------------------
Extracts QA scorecard *definitions* (the question sets/rubrics used to
grade agent interactions - sections, questions, answer options, scoring
weights) and loads a full snapshot into BigQuery.

Full refresh: scorecards are templates, not events - there are relatively
few of them, they change infrequently, and their nested section/question/
answer structure is exactly the kind of shape a full JSON load handles
cleanly without needing to diff nested arrays row by row.
"""

from typing import Any, Dict, List

from config import PlayvoxConfig, BigQueryConfig
from src.playvox_client import PlayvoxClient
from src.bigquery_loader import BigQueryLoader
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _get(item: dict, field: str, default=None):
    return item.get(field, default)


def transform(raw_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = []
    for item in raw_results:
        try:
            records.append({
                "status": _get(item, "status"),
                "team_ids": _get(item, "team_ids", []),
                "description": _get(item, "description"),
                "created_at_readable": _get(item, "created_at_readable"),
                "cloned_from": _get(item, "cloned_from"),
                "reference_name": _get(item, "reference_name"),
                "effectiveness_goal": _get(item, "effectiveness_goal"),
                "site_id": _get(item, "site_id"),
                "updated_at": _get(item, "updated_at"),
                "last_evaluation": _get(item, "last_evaluation"),
                "two_steps_approval": _get(item, "two_steps_approval"),
                "last_evaluation_readable": _get(item, "last_evaluation_readable"),
                "max_score": _get(item, "max_score"),
                "custom_fields": [
                    {
                        "_id": _get(f, "_id"),
                        "type": _get(f, "type"),
                        "name": _get(f, "name"),
                        "label": _get(f, "label"),
                        "values": _get(f, "values", []),
                    }
                    for f in _get(item, "custom_fields", [])
                ],
                "target": _get(item, "target"),
                "custom_reports": _get(item, "custom_reports", []),
                "name": _get(item, "name"),
                "total_questions": _get(item, "total_questions"),
                "created_at": _get(item, "created_at"),
                "updated_at_readable": _get(item, "updated_at_readable"),
                "created_by": _get(item, "created_by"),
                "pre_check_answers": _get(item, "pre_check_answers", []),
                "_id": _get(item, "_id"),
                "sections": [
                    {
                        "_id": _get(section, "_id"),
                        "max_score": _get(section, "max_score"),
                        "name": _get(section, "name"),
                        "custom_fields": _get(section, "custom_fields", []),
                        "questions": [
                            {
                                "_id": _get(question, "_id"),
                                "description": _get(question, "description"),
                                "weight": _get(question, "weight"),
                                "tip": _get(question, "tip"),
                                "answers": [
                                    {
                                        "_id": _get(answer, "_id"),
                                        "type": _get(answer, "type"),
                                        "value": _get(answer, "value"),
                                        "label": _get(answer, "label"),
                                    }
                                    for answer in _get(question, "answers", [])
                                ],
                                "comments": _get(question, "comments"),
                                "max_score": _get(question, "max_score"),
                            }
                            for question in _get(section, "questions", [])
                        ],
                    }
                    for section in _get(item, "sections", [])
                ],
            })
        except Exception as e:
            logger.error(f"Error transforming scorecard record: {e}. Item skipped.")
    return records


def run() -> None:
    playvox_cfg = PlayvoxConfig()
    bq_cfg = BigQueryConfig()

    client = PlayvoxClient(auth=playvox_cfg.auth)
    loader = BigQueryLoader(bq_cfg.credentials_path, bq_cfg.project_id, bq_cfg.dataset_id)

    logger.info("Fetching scorecards from Playvox...")
    raw_results = client.fetch_all_pages(playvox_cfg.endpoint("scorecards"))

    if not raw_results:
        logger.info("No scorecard data returned from Playvox. Nothing to load.")
        return

    records = transform(raw_results)
    rows_loaded = loader.load_json_truncate(records, bq_cfg.scorecards_table)
    logger.info(f"Scorecards pipeline complete. {rows_loaded} rows loaded into {bq_cfg.scorecards_table}.")


if __name__ == "__main__":
    run()
