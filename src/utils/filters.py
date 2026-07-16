"""
filters.py
----------
Shared helper for the "only sync records touched in the last N days"
pattern used by several pipelines (users, roles, teams, coachings,
evaluations). Previously this date-window logic was reimplemented five
different ways - raw string slicing (`"T" in date`), `datetime.strptime`
with a manual fallback format, and `dateutil.parser` - with subtly
different edge-case behavior. One implementation, one behavior.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as date_parser


def _parse(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = date_parser.parse(value)
        return parsed.replace(tzinfo=None)  # normalize to naive for comparison
    except (ValueError, TypeError):
        return None


def filter_recent(
    records: List[Dict[str, Any]],
    days: int,
    date_fields: Tuple[str, ...] = ("created_at", "updated_at"),
) -> List[Dict[str, Any]]:
    """
    Keep only records where the most recent of `date_fields` falls within
    the last `days` days.
    """
    end = datetime.now()
    start = end - timedelta(days=days)

    kept = []
    for record in records:
        candidate_dates = [d for d in (_parse(record.get(f)) for f in date_fields) if d is not None]
        if not candidate_dates:
            continue
        most_recent = max(candidate_dates)
        if start <= most_recent <= end:
            kept.append(record)

    return kept
