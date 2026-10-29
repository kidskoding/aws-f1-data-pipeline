"""AWS Lambda entry point for the warehouse load step.

1. Makes sure the Spectrum schema (Redshift's window into the Glue Data Catalog) exists.
2. Creates tables if needed, then replaces their contents from curated Parquet,
   all in ONE transaction: readers see the old data or the new data, never half of each.
3. Returns row counts, so Step Functions can reconcile the warehouse against the lake.

Environment: WORKGROUP, DATABASE, SECRET_ARN, DATA_BUCKET, CURATED_PREFIX, CURATED_DATABASE.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import boto3

from f1_pipeline.logs import configure_logging
from f1_pipeline.warehouse import RedshiftData, read_sql, render, split_statements

configure_logging()
log = logging.getLogger(__name__)


def load(warehouse: RedshiftData, bucket: str, curated_prefix: str, curated_database: str) -> dict[str, Any]:
    params = {"bucket": bucket, "curated_prefix": curated_prefix.strip("/"), "curated_database": curated_database}

    # CREATE EXTERNAL SCHEMA cannot run inside a transaction, so it runs on its own first.
    for statement in split_statements(render(read_sql("warehouse", "00_spectrum.sql"), **params)):
        warehouse.run(statement.sql)

    transaction = [
        s.sql
        for name in ("01_schema.sql", "02_load.sql")
        for s in split_statements(render(read_sql("warehouse", name), **params))
    ]
    warehouse.wait(warehouse.batch(transaction))
    log.info("loaded warehouse in one transaction of %d statements", len(transaction))

    (counts,) = split_statements(read_sql("warehouse", "03_row_counts.sql"))
    _, rows = warehouse.query(counts.sql)
    tables = {str(name): int(count) for name, count in rows}
    log.info("row counts: %s", tables)
    # Strings, because Step Functions compares this to Athena's result, which is always a string.
    return {"tables": tables, "results_rows": str(tables.get("results", 0))}


def handler(_event: dict[str, Any], _context: object) -> dict[str, Any]:
    warehouse = RedshiftData(
        boto3.client("redshift-data"),
        workgroup=os.environ["WORKGROUP"],
        database=os.environ["DATABASE"],
        secret_arn=os.environ["SECRET_ARN"],
    )
    return load(
        warehouse,
        bucket=os.environ["DATA_BUCKET"],
        curated_prefix=os.environ.get("CURATED_PREFIX", "curated"),
        curated_database=os.environ["CURATED_DATABASE"],
    )
