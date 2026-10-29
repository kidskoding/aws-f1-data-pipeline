"""Run the analytics SQL and print the results.

    python -m f1_pipeline.query                # against Redshift Serverless (reads Terraform outputs)
    python -m f1_pipeline.query --local        # against local Parquet with DuckDB, no AWS needed
    python -m f1_pipeline.query 02_championship.sql

The same SQL files run in both places: the local mode maps the `f1` and `lake`
schemas onto data/curated/<table>/*.parquet.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from f1_pipeline.warehouse import RedshiftData, list_sql, read_sql, split_statements

OUTPUTS_FILE = Path("build/terraform-outputs.json")  # written by `make deploy`
Runner = Callable[[str], tuple[list[str], list[list[Any]]]]


def format_table(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    cells = [[_fmt(v) for v in row] for row in rows]
    widths = [max(len(c), *(len(r[i]) for r in cells)) if cells else len(c) for i, c in enumerate(columns)]
    numeric = [
        all(_is_number(row[i]) for row in rows if row[i] is not None) and bool(rows) for i in range(len(columns))
    ]

    def line(values: Sequence[str]) -> str:
        return "  ".join(v.rjust(w) if num else v.ljust(w) for v, w, num in zip(values, widths, numeric, strict=True))

    return "\n".join([line(columns), "  ".join("-" * w for w in widths), *(line(r) for r in cells)])


def _fmt(value: Any) -> str:
    return "" if value is None else str(value)


def _is_number(value: Any) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return not isinstance(value, bool)


def redshift_runner(outputs_file: Path) -> Runner:
    import boto3

    if not outputs_file.exists():
        raise SystemExit(f"{outputs_file} not found. Run `make deploy` first.")
    outputs = {name: item["value"] for name, item in json.loads(outputs_file.read_text()).items()}
    warehouse = RedshiftData(
        boto3.client("redshift-data"),
        workgroup=outputs["workgroup_name"],
        database=outputs["database_name"],
        secret_arn=outputs["admin_secret_arn"],
    )
    return warehouse.query


def duckdb_runner(curated: Path) -> Runner:
    import duckdb

    con = duckdb.connect()
    for schema in ("f1", "lake"):
        con.execute(f"CREATE SCHEMA {schema}")
        for table_dir in sorted(p for p in curated.iterdir() if p.is_dir()):
            con.execute(f"CREATE VIEW {schema}.{table_dir.name} AS SELECT * FROM read_parquet('{table_dir}/*.parquet')")

    def run(sql: str) -> tuple[list[str], list[list[Any]]]:
        cursor = con.execute(sql)
        return [d[0] for d in cursor.description], [list(r) for r in cursor.fetchall()]

    return run


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the analytics queries.")
    parser.add_argument("files", nargs="*", help="analytics SQL file names (default: all)")
    parser.add_argument("--local", action="store_true", help="query local Parquet with DuckDB instead of Redshift")
    parser.add_argument("--curated", default="data/curated", help="local curated folder for --local")
    args = parser.parse_args()

    run = duckdb_runner(Path(args.curated)) if args.local else redshift_runner(OUTPUTS_FILE)
    for name in args.files or list_sql("analytics"):
        for statement in split_statements(read_sql("analytics", name)):
            columns, rows = run(statement.sql)
            print(f"\n{statement.title or name}\n")
            print(format_table(columns, rows))


if __name__ == "__main__":
    main()
