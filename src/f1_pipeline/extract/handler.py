"""AWS Lambda entry point for the extract step.

Event: {"seasons": "2021-current"} (default: the current season only, for scheduled runs).
Environment: DATA_BUCKET (required), RAW_PREFIX (default "raw").
"""

from __future__ import annotations

import os
from typing import Any

import boto3

from f1_pipeline.extract import extract
from f1_pipeline.extract.jolpica import JolpicaClient, parse_seasons
from f1_pipeline.extract.sinks import S3Sink
from f1_pipeline.logs import configure_logging

configure_logging()


def handler(event: dict[str, Any], _context: object) -> dict[str, object]:
    seasons = parse_seasons(str(event.get("seasons") or "current"))
    sink = S3Sink(boto3.client("s3"), os.environ["DATA_BUCKET"], os.environ.get("RAW_PREFIX", "raw"))
    return extract(JolpicaClient(), sink, seasons).as_dict()
