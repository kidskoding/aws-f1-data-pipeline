"""Extract step: copy F1 data from the API into the raw zone, unchanged."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from f1_pipeline.extract.jolpica import ENDPOINTS, JolpicaClient
from f1_pipeline.extract.sinks import Sink

log = logging.getLogger(__name__)


@dataclass
class ExtractSummary:
    seasons: list[int]
    pages: dict[str, int] = field(default_factory=dict)  # "results/2025" -> page count
    requests: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "seasons": self.seasons,
            "pages": self.pages,
            "requests": self.requests,
            "total_pages": sum(self.pages.values()),
        }


def extract(client: JolpicaClient, sink: Sink, seasons: list[int], datasets: list[str] | None = None) -> ExtractSummary:
    """Write every page of every dataset for each season. Reruns replace a season wholesale."""
    summary = ExtractSummary(seasons=seasons)
    for season in seasons:
        for dataset in datasets or list(ENDPOINTS):
            prefix = f"{dataset}/{season}"
            sink.replace(prefix)
            count = 0
            for page in client.pages(dataset, season):
                sink.put(page.key, json.dumps(page.body, separators=(",", ":")).encode())
                count += 1
            summary.pages[prefix] = count
            log.info("extracted %s: %d page(s)", prefix, count)
    summary.requests = client.requests_made
    return summary
