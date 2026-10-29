import pytest

from f1_pipeline.load.handler import load
from f1_pipeline.warehouse import RedshiftData, WarehouseError, list_sql, read_sql, render, split_statements


class FakeDataApi:
    """Records Data API calls. Statements finish after one poll unless told to fail."""

    def __init__(self, fail_on=None, results=None):
        self.calls = []
        self.fail_on = fail_on
        self.results = results or {}
        self._sql = {}

    def execute_statement(self, Sql, **target):
        self.calls.append(("execute", Sql))
        sid = f"s{len(self.calls)}"
        self._sql[sid] = Sql
        return {"Id": sid}

    def batch_execute_statement(self, Sqls, **target):
        self.calls.append(("batch", Sqls))
        sid = f"s{len(self.calls)}"
        self._sql[sid] = " ".join(Sqls)
        return {"Id": sid}

    def describe_statement(self, Id):
        sql = self._sql[Id]
        if self.fail_on and self.fail_on in sql:
            return {"Id": Id, "Status": "FAILED", "Error": "boom"}
        return {"Id": Id, "Status": "FINISHED", "HasResultSet": sql.lstrip().upper().startswith("SELECT")}

    def get_statement_result(self, Id, **_):
        columns, rows = self.results.get("counts", ([], []))
        return {
            "ColumnMetadata": [{"name": c} for c in columns],
            "Records": [[{"stringValue": r[0]}, {"longValue": r[1]}] for r in rows],
        }

    def cancel_statement(self, Id):
        self.calls.append(("cancel", Id))


def warehouse(api, **kw):
    return RedshiftData(api, workgroup="wg", database="dev", secret_arn="arn:secret", sleep=lambda s: None, **kw)


def test_split_statements_uses_leading_comments_as_titles():
    statements = split_statements("-- First thing\nSELECT 1;\n\n-- Second\n-- thing\nSELECT 2;\n")
    assert [(s.title, s.sql) for s in statements] == [("First thing", "SELECT 1"), ("Second thing", "SELECT 2")]


def test_render_fills_placeholders_and_refuses_missing_ones():
    assert render("s3://{{bucket}}/{{prefix}}/", bucket="b", prefix="p") == "s3://b/p/"
    with pytest.raises(KeyError, match="prefix"):
        render("s3://{{bucket}}/{{prefix}}/", bucket="b")


def test_every_packaged_sql_file_renders():
    params = {"bucket": "b", "curated_prefix": "curated", "curated_database": "f1_curated"}
    for group in ("warehouse", "analytics"):
        for name in list_sql(group):
            assert split_statements(render(read_sql(group, name), **params)), name


def test_failed_statement_raises_with_redshift_error():
    with pytest.raises(WarehouseError, match="boom"):
        warehouse(FakeDataApi(fail_on="SELECT")).run("SELECT 1")


def test_slow_statement_is_cancelled_at_timeout():
    class NeverFinishes(FakeDataApi):
        def describe_statement(self, Id):
            return {"Id": Id, "Status": "STARTED"}

    api = NeverFinishes()
    with pytest.raises(WarehouseError, match="timed out"):
        warehouse(api, poll_seconds=1, timeout_seconds=3).run("SELECT 1")
    assert api.calls[-1] == ("cancel", "s1")


def test_load_runs_spectrum_then_one_transaction_then_counts():
    api = FakeDataApi(results={"counts": (["table_name", "row_count"], [("races", 130), ("results", 3286)])})
    out = load(warehouse(api), bucket="lake", curated_prefix="curated/", curated_database="f1_curated")

    kinds = [kind for kind, _ in api.calls]
    assert kinds == ["execute", "batch", "execute"]
    assert "CREATE EXTERNAL SCHEMA IF NOT EXISTS lake" in api.calls[0][1]
    batch = api.calls[1][1]
    assert batch[0] == "CREATE SCHEMA IF NOT EXISTS f1"
    assert "COPY f1.results FROM 's3://lake/curated/results/'" in " ".join(batch)
    assert batch.index("DELETE FROM f1.results") < next(
        i for i, s in enumerate(batch) if s.startswith("COPY f1.results")
    )
    assert out == {"tables": {"races": 130, "results": 3286}, "results_rows": "3286"}


def test_load_failure_surfaces_instead_of_returning_counts():
    with pytest.raises(WarehouseError):
        load(
            warehouse(FakeDataApi(fail_on="COPY")),
            bucket="lake",
            curated_prefix="curated",
            curated_database="f1_curated",
        )
