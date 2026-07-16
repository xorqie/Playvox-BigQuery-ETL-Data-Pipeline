"""
playvox_client.py
--------------------
A thin, reusable client around the Playvox REST API (v1).

Every one of the original 8 scripts reimplemented this same fetch loop
with small, inconsistent variations - different retry counts, one script
capped itself at a hardcoded 5 pages using `asyncio` (silently truncating
data), and rate-limit handling was copy-pasted with minor drift. This
client is the single implementation all pipelines now share.

Pagination: Playvox signals more pages via a `next_page` boolean in the
JSON body (unlike, e.g., a `Link` header), so we follow that instead.

Rate limiting: Playvox returns `X-RateLimit-Remaining` / `X-RateLimit-Reset`
headers on 429s, which we respect instead of guessing a sleep duration.
"""

import time
from typing import Any, Dict, List, Optional

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 30
MAX_RETRIES = 5


class PlayvoxClient:
    def __init__(self, auth: tuple, requests_per_minute: int = 100):
        self.auth = auth
        self.headers = {"Content-Type": "application/json"}
        self.requests_per_minute = requests_per_minute
        self._requests_made = 0
        self._window_start = time.time()

    def _throttle(self) -> None:
        """Simple client-side rate limiting to avoid tripping Playvox's own limits."""
        if self._requests_made >= self.requests_per_minute:
            elapsed = time.time() - self._window_start
            if elapsed < 60:
                wait_time = 60 - elapsed
                logger.info(f"Approaching rate limit. Pausing {int(wait_time)}s before continuing...")
                time.sleep(wait_time)
            self._window_start = time.time()
            self._requests_made = 0

    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()
            response = requests.get(url, auth=self.auth, headers=self.headers, timeout=DEFAULT_TIMEOUT)
            self._requests_made += 1

            if response.status_code == 200:
                return response

            if response.status_code == 429:
                remaining = int(response.headers.get("X-RateLimit-Remaining", 0))
                reset_time = int(response.headers.get("X-RateLimit-Reset", time.time()))
                if remaining == 0:
                    wait_time = max(reset_time - time.time(), 0) + 1
                    logger.warning(f"Rate limited by Playvox. Sleeping {int(wait_time)}s until reset...")
                    time.sleep(wait_time)
                else:
                    time.sleep(2 ** attempt)
                continue

            if response.status_code >= 500 and attempt < MAX_RETRIES:
                sleep_time = 2 ** attempt
                logger.warning(
                    f"Playvox returned {response.status_code}. Retrying in {sleep_time}s "
                    f"(attempt {attempt}/{MAX_RETRIES})..."
                )
                time.sleep(sleep_time)
                continue

            logger.error(f"Playvox request failed [{response.status_code}]: {url}")
            return None

        logger.error(f"Exceeded max retries fetching {url}")
        return None

    def fetch_all_pages(self, endpoint_template: str) -> List[Dict[str, Any]]:
        """
        Fetch every page of a Playvox list endpoint.

        `endpoint_template` must contain a `{page}` placeholder, e.g.
        `.../api/v1/users?page={page}&per_page=100`.
        """
        page = 1
        all_results: List[Dict[str, Any]] = []

        while True:
            url = endpoint_template.format(page=page)
            response = self._get(url)
            if response is None:
                break

            payload = response.json()
            page_results = payload.get("result", [])
            all_results.extend(page_results)
            logger.info(f"Page {page}: fetched {len(page_results)} records ({len(all_results)} total so far)")

            if payload.get("next_page"):
                page += 1
            else:
                break

        return all_results
