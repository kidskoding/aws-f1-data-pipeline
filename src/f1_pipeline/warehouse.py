"""Run SQL on Redshift Serverless through the Redshift Data API.

The Data API runs SQL over HTTPS with IAM auth: no JDBC driver, no open port,
and no password in code (the admin secret lives in Secrets Manager).
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from importlib import resources
from typing import Any

_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")


class WarehouseError(RuntimeError):
    """A statement failed, was aborted, or timed out."""


@dataclass(frozen=True)
class Statement:
    title: str
    sql: str


def read_sql(group: str, name: str) -> str:
    """Read a packaged SQL file, e.g. read_sql("warehouse", "02_load.sql")."""
    return resources.files("f1_pipeline.sql").joinpath(group, name).read_text()


def list_sql(group: str) -> list[str]:
    return sorted(
        p.name for p in resources.files("f1_pipeline.sql").joinpath(group).iterdir() if p.name.endswith(".sql")
    )


def render(sql: str, **params: str) -> str:
    """Fill {{name}} placeholders. Every placeholder must be supplied."""
    missing = {m for m in _PLACEHOLDER.findall(sql) if m not in params}
    if missing:
        raise KeyError(f"SQL placeholders without values: {sorted(missing)}")
    return _PLACEHOLDER.sub(lambda m: params[m.group(1)], sql)


def split_statements(text: str) -> list[Statement]:
    """Split on semicolons. A statement's leading `-- comment` lines become its title."""
    statements = []
    for chunk in text.split(";"):
        lines = [line for line in chunk.strip().splitlines() if line.strip()]
        comments = [line.strip().removeprefix("--").strip() for line in lines if line.lstrip().startswith("--")]
        sql = "\n".join(line for line in lines if not line.lstrip().startswith("--")).strip()
        if sql:
            statements.append(Statement(title=" ".join(comments), sql=sql))
    return statements


class RedshiftData:
    def __init__(
        self,
        client: Any,
        workgroup: str,
        database: str,
        secret_arn: str,
        poll_seconds: float = 1.0,
        timeout_seconds: float = 600.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.client = client
        self.target = {"WorkgroupName": workgroup, "Database": database, "SecretArn": secret_arn}
        self.poll_seconds = poll_seconds
        self.timeout_seconds = timeout_seconds
        self._sleep = sleep

    def execute(self, sql: str) -> str:
        return str(self.client.execute_statement(Sql=sql, **self.target)["Id"])

    def batch(self, sqls: list[str]) -> str:
        """Run statements in order as ONE transaction: all succeed or none do."""
        return str(self.client.batch_execute_statement(Sqls=sqls, **self.target)["Id"])

    def wait(self, statement_id: str) -> dict[str, Any]:
        waited = 0.0
        while True:
            desc: dict[str, Any] = self.client.describe_statement(Id=statement_id)
            status = desc["Status"]
            if status == "FINISHED":
                return desc
            if status in ("FAILED", "ABORTED"):
                raise WarehouseError(f"Statement {status.lower()}: {desc.get('Error', 'no error message')}")
            if waited >= self.timeout_seconds:
                self.client.cancel_statement(Id=statement_id)
                raise WarehouseError(f"Statement timed out after {self.timeout_seconds:.0f}s")
            self._sleep(self.poll_seconds)
            waited += self.poll_seconds

    def run(self, sql: str) -> dict[str, Any]:
        return self.wait(self.execute(sql))

    def query(self, sql: str) -> tuple[list[str], list[list[Any]]]:
        """Run a SELECT and return (column names, rows)."""
        desc = self.run(sql)
        if not desc.get("HasResultSet"):
            return [], []
        columns: list[str] = []
        rows: list[list[Any]] = []
        token = None
        while True:
            kwargs = {"Id": desc["Id"], **({"NextToken": token} if token else {})}
            page = self.client.get_statement_result(**kwargs)
            columns = columns or [c["name"] for c in page["ColumnMetadata"]]
            rows.extend([_cell(cell) for cell in record] for record in page["Records"])
            token = page.get("NextToken")
            if not token:
                return columns, rows


def _cell(cell: dict[str, Any]) -> Any:
    if cell.get("isNull"):
        return None
    return next(iter(cell.values()))
