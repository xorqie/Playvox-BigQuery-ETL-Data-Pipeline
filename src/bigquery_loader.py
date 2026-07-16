"""
bigquery_loader.py
--------------------
Reusable BigQuery load helpers shared by every pipeline.

The original 8 scripts each invented their own upload strategy: a manual
"query existing IDs, split into new/updated, WRITE_APPEND everything
anyway" pattern (users, evaluations), a "DELETE matching IDs then
WRITE_APPEND" pattern (teams), and a "query rows outside the sync window,
concat in pandas, WRITE_TRUNCATE the combined result" pattern (roles,
coachings). All three are trying to do the same thing - upsert - through
increasingly indirect means, and two of them (users, evaluations) have a
bug: they compute `new_data`/`updated_data` for logging purposes but then
upload `bq_data` (everything) with WRITE_APPEND regardless, which
duplicates every existing row on every run.

This module replaces all of that with two clear, correct strategies:

1. `load_merge` — a staging-table + SQL `MERGE`, which is the standard,
   atomic way to upsert in BigQuery. Used for every resource with a flat,
   tabular shape (users, teams, roles, coachings, evaluations).

2. `load_json_truncate` — a full-refresh JSON load. Used for resources
   with deeply nested/repeated fields (interactions, scorecards,
   calibrations) where a `MERGE` would need to reconstruct nested arrays
   in SQL for little benefit, and a full snapshot is simple and correct.

`fetch_lookup_map` centralizes the "look up a name from an ID by querying
BigQuery" pattern that appeared in coachings/evaluations - previously done
by pulling the whole users/teams table into a throwaway local SQLite
database. A Python dict is sufficient at this data volume and removes an
entire (unnecessary) dependency and moving part.
"""

from typing import Any, Dict, List

import pandas as pd
from google.cloud import bigquery

from src.utils.logger import get_logger

logger = get_logger(__name__)


class BigQueryLoader:
    def __init__(self, credentials_path: str, project_id: str, dataset_id: str):
        self.client = bigquery.Client.from_service_account_json(credentials_path)
        self.project_id = project_id
        self.dataset_id = dataset_id

    def _table_ref(self, table_id: str) -> str:
        return f"{self.project_id}.{self.dataset_id}.{table_id}"

    def fetch_lookup_map(self, table_id: str, id_col: str, name_expr: str) -> Dict[str, str]:
        """
        Build an {id: display_name} dict from an existing BigQuery table.
        `name_expr` is a SQL expression, e.g. "name" or "CONCAT(name, ' ', last_name)".
        Returns an empty dict (with a warning) if the table doesn't exist yet or the
        query fails, so a pipeline can still run before its dependency tables are populated.
        """
        query = f"SELECT {id_col} AS id, {name_expr} AS display_name FROM `{self._table_ref(table_id)}`"
        try:
            rows = self.client.query(query).result()
            return {row.id: row.display_name for row in rows if row.id is not None}
        except Exception as e:
            logger.warning(f"Could not build lookup map from {table_id}: {e}")
            return {}

    def load_merge(self, df: pd.DataFrame, table_id: str, merge_key: str = "_id") -> int:
        """Upsert `df` into `table_id` via a staging table + MERGE on `merge_key`."""
        if df.empty:
            logger.info("No data to upload; skipping load.")
            return 0

        staging_table_id = f"staging_{table_id}"
        staging_ref = self._table_ref(staging_table_id)
        target_ref = self._table_ref(table_id)

        try:
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = self.client.load_table_from_dataframe(df, staging_ref, job_config=job_config)
            job.result()
            logger.info(f"Staged {len(df)} rows into {staging_ref}.")

            columns: List[str] = list(df.columns)
            update_clause = ", ".join(f"target.{c} = source.{c}" for c in columns if c != merge_key)
            insert_columns = ", ".join(columns)
            insert_values = ", ".join(f"source.{c}" for c in columns)

            merge_query = f"""
                MERGE `{target_ref}` AS target
                USING `{staging_ref}` AS source
                ON target.{merge_key} = source.{merge_key}
                WHEN MATCHED THEN
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """
            self.client.query(merge_query).result()
            logger.info(f"Merged {len(df)} rows into {target_ref}.")
            return len(df)
        finally:
            self.client.delete_table(staging_ref, not_found_ok=True)

    def load_json_truncate(self, records: List[Dict[str, Any]], table_id: str) -> int:
        """Full-refresh load of nested JSON records (WRITE_TRUNCATE, schema auto-detected)."""
        if not records:
            logger.info("No data to upload; skipping load.")
            return 0

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )
        job = self.client.load_table_from_json(records, self._table_ref(table_id), job_config=job_config)
        job.result()
        logger.info(f"Loaded {len(records)} rows into {self._table_ref(table_id)} (full refresh).")
        return len(records)
