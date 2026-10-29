import json

import boto3
from moto import mock_aws

from f1_pipeline.extract import extract
from f1_pipeline.extract.jolpica import Page
from f1_pipeline.extract.sinks import LocalSink, S3Sink


class FakeClient:
    """Two pages per dataset per season."""

    requests_made = 0

    def pages(self, dataset, season):
        for n in range(2):
            self.requests_made += 1
            yield Page(dataset, season, n, {"MRData": {"total": "150", "page": n}})


def test_extract_writes_every_page_and_summarizes(tmp_path):
    summary = extract(FakeClient(), LocalSink(tmp_path), [2025], datasets=["results", "sprint"])
    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.json"))
    assert files == [
        "results/2025/page-000.json",
        "results/2025/page-001.json",
        "sprint/2025/page-000.json",
        "sprint/2025/page-001.json",
    ]
    assert json.loads((tmp_path / "results/2025/page-001.json").read_text())["MRData"]["page"] == 1
    assert summary.as_dict() == {
        "seasons": [2025],
        "pages": {"results/2025": 2, "sprint/2025": 2},
        "requests": 4,
        "total_pages": 4,
    }


def test_rerun_replaces_a_season_instead_of_mixing_pages(tmp_path):
    stale = tmp_path / "results/2025/page-009.json"
    stale.parent.mkdir(parents=True)
    stale.write_text("{}")
    other_season = tmp_path / "results/2024/page-000.json"
    other_season.parent.mkdir(parents=True)
    other_season.write_text("{}")
    extract(FakeClient(), LocalSink(tmp_path), [2025], datasets=["results"])
    assert not stale.exists()
    assert other_season.exists()


@mock_aws
def test_s3_sink_replaces_prefix_and_writes_under_raw():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="lake")
    s3.put_object(Bucket="lake", Key="raw/results/2025/page-009.json", Body=b"{}")
    s3.put_object(Bucket="lake", Key="raw/results/2025-old/keep.json", Body=b"{}")

    extract(FakeClient(), S3Sink(s3, "lake", "raw"), [2025], datasets=["results"])

    keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket="lake")["Contents"])
    assert keys == [
        "raw/results/2025-old/keep.json",
        "raw/results/2025/page-000.json",
        "raw/results/2025/page-001.json",
    ]
