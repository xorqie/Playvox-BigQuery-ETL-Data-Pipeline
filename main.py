"""
main.py
--------
Single entrypoint for running the Playvox -> BigQuery pipelines.

Usage:
    python main.py                    # run every pipeline, in dependency order
    python main.py users teams        # run a specific subset
    python main.py evaluations        # run just one

Dependency order matters for two pipelines: `coachings` and `evaluations`
enrich their records with agent/team names by querying the `users` and
`teams` tables in BigQuery, so those two should run *after* users/teams
have loaded. Running `python main.py` with no arguments handles this
automatically; if you run a subset manually, keep that order in mind.
"""

import sys

from src.pipelines import (
    users_pipeline,
    teams_pipeline,
    roles_pipeline,
    interactions_pipeline,
    scorecards_pipeline,
    calibrations_pipeline,
    coachings_pipeline,
    evaluations_pipeline,
)
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Order reflects dependencies: users/teams before the pipelines that look up names from them.
PIPELINES = {
    "users": users_pipeline.run,
    "teams": teams_pipeline.run,
    "roles": roles_pipeline.run,
    "interactions": interactions_pipeline.run,
    "scorecards": scorecards_pipeline.run,
    "calibrations": calibrations_pipeline.run,
    "coachings": coachings_pipeline.run,
    "evaluations": evaluations_pipeline.run,
}


def main() -> None:
    requested = sys.argv[1:] or list(PIPELINES.keys())

    unknown = [name for name in requested if name not in PIPELINES]
    if unknown:
        logger.error(f"Unknown pipeline(s): {unknown}. Available: {list(PIPELINES.keys())}")
        sys.exit(1)

    for name in requested:
        logger.info(f"--- Starting '{name}' pipeline ---")
        try:
            PIPELINES[name]()
        except Exception:
            logger.exception(f"Pipeline '{name}' failed.")
            raise
        logger.info(f"--- Finished '{name}' pipeline ---")


if __name__ == "__main__":
    main()
