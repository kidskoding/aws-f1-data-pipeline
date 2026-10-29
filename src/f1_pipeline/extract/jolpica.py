"""Client for the Jolpica F1 API (https://api.jolpi.ca), the community successor to Ergast.

Standard library only, so it runs unchanged on a laptop and in AWS Lambda.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date
from typing import Any

log = logging.getLogger(__name__)

BASE_URL = "https://api.jolpi.ca/ergast/f1"
USER_AGENT = "aws-f1-data-pipeline/0.1 (personal data engineering project)"
PAGE_SIZE = 100  # the API's maximum page size

ENDPOINTS: dict[str, str] = {
    "results": "results",
    "sprint": "sprint",
    "driver_standings": "driverstandings",
}

Opener = Callable[[urllib.request.Request, float], Any]


class ExtractError(RuntimeError):
    """The API could not be read after all retries."""


@dataclass(frozen=True)
class Page:
    dataset: str
    season: int
    number: int
    body: dict[str, Any]

    @property
    def key(self) -> str:
        """Storage key relative to the raw zone, e.g. results/2025/page-000.json."""
        return f"{self.dataset}/{self.season}/page-{self.number:03d}.json"


class JolpicaClient:
    """Throttled, retrying, paginating reader for the Jolpica API.

    The API allows bursts of 4 requests per second and 500 per hour, so requests are
    spaced at least `min_interval` seconds apart, and 429 or 5xx responses back off.
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        min_interval: float = 0.3,
        max_retries: int = 5,
        timeout: float = 30.0,
        opener: Opener | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.timeout = timeout
        self._open = opener or (lambda request, timeout: urllib.request.urlopen(request, timeout=timeout))
        self._sleep = sleep
        self._clock = clock
        self._last_request: float | None = None
        self.requests_made = 0

    def get_json(self, url: str) -> dict[str, Any]:
        for attempt in range(self.max_retries):
            self._throttle()
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            try:
                with self._open(request, self.timeout) as response:
                    self.requests_made += 1
                    body: dict[str, Any] = json.load(response)
                    return body
            except urllib.error.HTTPError as e:
                if e.code != 429 and e.code < 500:
                    raise ExtractError(f"HTTP {e.code} for {url}") from e
                delay = _retry_after(e) or 2.0**attempt
                log.warning("HTTP %s from API, retrying in %.0fs (attempt %d)", e.code, delay, attempt + 1)
            except urllib.error.URLError as e:
                delay = 2.0**attempt
                log.warning("Network error %s, retrying in %.0fs (attempt %d)", e.reason, delay, attempt + 1)
            self._sleep(delay)
        raise ExtractError(f"Gave up after {self.max_retries} attempts: {url}")

    def pages(self, dataset: str, season: int) -> Iterator[Page]:
        """Every page of one dataset for one season, in order."""
        path = ENDPOINTS[dataset]
        offset, number = 0, 0
        while True:
            body = self.get_json(f"{self.base_url}/{season}/{path}/?limit={PAGE_SIZE}&offset={offset}")
            yield Page(dataset=dataset, season=season, number=number, body=body)
            total = int(body["MRData"]["total"])
            offset, number = offset + PAGE_SIZE, number + 1
            if offset >= total:
                return

    def _throttle(self) -> None:
        if self._last_request is not None:
            wait = self.min_interval - (self._clock() - self._last_request)
            if wait > 0:
                self._sleep(wait)
        self._last_request = self._clock()


def _retry_after(error: urllib.error.HTTPError) -> float | None:
    value = error.headers.get("Retry-After") if error.headers else None
    try:
        return float(value) if value else None
    except ValueError:
        return None


def parse_seasons(spec: str, today: date | None = None) -> list[int]:
    """'2021-2026', '2023,2025', 'current', or '2021-current' to a list of seasons."""
    current = (today or date.today()).year
    spec = spec.strip().lower().replace("current", str(current))
    if "-" in spec:
        start, end = (int(part) for part in spec.split("-", 1))
        if start > end:
            raise ValueError(f"Season range is backwards: {spec}")
        return list(range(start, end + 1))
    return sorted({int(part) for part in spec.split(",") if part})
