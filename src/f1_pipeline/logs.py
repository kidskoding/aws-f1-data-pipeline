"""JSON logging, so CloudWatch Logs Insights can filter and aggregate on fields."""

from __future__ import annotations

import json
import logging
import os


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def configure_logging(level: str | None = None) -> None:
    root = logging.getLogger()
    root.setLevel(level or os.environ.get("LOG_LEVEL", "INFO"))
    for handler in root.handlers:  # Lambda installs its own handler; reuse it with our format
        handler.setFormatter(JsonFormatter())
    if not root.handlers:
        stream = logging.StreamHandler()
        stream.setFormatter(JsonFormatter())
        root.addHandler(stream)
