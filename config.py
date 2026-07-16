"""
config.py
---------
Centralized, environment-driven configuration for the Playvox -> BigQuery
pipeline. No secrets live in source code - everything is read from
environment variables. Copy `.env.example` to `.env` and fill in your own
values, or export the variables directly in your shell / CI system.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable: '{name}'. "
            f"See .env.example for the full list of required variables."
        )
    return value


@dataclass(frozen=True)
class BigQueryConfig:
    project_id: str = field(default_factory=lambda: _require_env("BQ_PROJECT_ID"))
    dataset_id: str = field(default_factory=lambda: os.getenv("BQ_DATASET_ID", "playvox"))
    credentials_path: str = field(default_factory=lambda: _require_env("GOOGLE_APPLICATION_CREDENTIALS"))

    users_table: str = field(default_factory=lambda: os.getenv("BQ_USERS_TABLE", "users"))
    teams_table: str = field(default_factory=lambda: os.getenv("BQ_TEAMS_TABLE", "teams"))
    roles_table: str = field(default_factory=lambda: os.getenv("BQ_ROLES_TABLE", "roles"))
    interactions_table: str = field(default_factory=lambda: os.getenv("BQ_INTERACTIONS_TABLE", "interactions"))
    scorecards_table: str = field(default_factory=lambda: os.getenv("BQ_SCORECARDS_TABLE", "scorecards"))
    calibrations_table: str = field(default_factory=lambda: os.getenv("BQ_CALIBRATIONS_TABLE", "calibrations"))
    evaluations_table: str = field(default_factory=lambda: os.getenv("BQ_EVALUATIONS_TABLE", "evaluations"))
    coachings_table: str = field(default_factory=lambda: os.getenv("BQ_COACHINGS_TABLE", "coachings"))


@dataclass(frozen=True)
class PlayvoxConfig:
    domain: str = field(default_factory=lambda: _require_env("PLAYVOX_DOMAIN"))
    api_key: str = field(default_factory=lambda: _require_env("PLAYVOX_API_KEY"))

    @property
    def base_url(self) -> str:
        return f"https://{self.domain}/api/v1"

    @property
    def auth(self) -> tuple:
        """Playvox uses HTTP Basic Auth with the API key split on ':' into user/pass."""
        key_id, secret = self.api_key.split(":")
        return key_id, secret

    def endpoint(self, resource: str, per_page: int = 100) -> str:
        return f"{self.base_url}/{resource}?include=&page={{page}}&per_page={per_page}&query=&fields=&sort="


# How many days back to look when a pipeline runs an incremental (recent-window) sync.
INCREMENTAL_WINDOW_DAYS = int(os.getenv("INCREMENTAL_WINDOW_DAYS", "3"))

bigquery_config = BigQueryConfig
playvox_config = PlayvoxConfig
