import io
import json
import urllib.error
from datetime import date
from email.message import Message

import pytest

from f1_pipeline.extract.jolpica import ExtractError, JolpicaClient, Page, parse_seasons


class FakeApi:
    """Stands in for urllib: returns queued responses and records requested URLs."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.urls = []

    def __call__(self, request, timeout):
        self.urls.append(request.full_url)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return io.BytesIO(json.dumps(response).encode())


def page(total):
    return {"MRData": {"total": str(total)}}


def http_error(code, retry_after=None):
    headers = Message()
    if retry_after:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError("https://api", code, "error", headers, None)


def client(api, sleeps=None):
    return JolpicaClient(
        base_url="https://api",
        opener=api,
        sleep=(sleeps.append if sleeps is not None else lambda s: None),
        clock=lambda: 0.0,
    )


def test_pages_follow_offsets_until_total():
    api = FakeApi(page(250), page(250), page(250))
    pages = list(client(api).pages("results", 2025))
    assert [p.number for p in pages] == [0, 1, 2]
    assert api.urls == [
        "https://api/2025/results/?limit=100&offset=0",
        "https://api/2025/results/?limit=100&offset=100",
        "https://api/2025/results/?limit=100&offset=200",
    ]


def test_single_page_when_total_fits():
    api = FakeApi(page(21))
    assert len(list(client(api).pages("driver_standings", 2025))) == 1
    assert api.urls == ["https://api/2025/driverstandings/?limit=100&offset=0"]


def test_requests_are_throttled():
    sleeps = []
    c = client(FakeApi(page(1), page(1)), sleeps)
    c.get_json("https://api/a")
    c.get_json("https://api/b")
    assert sleeps == [pytest.approx(0.3)]  # second request waits; the clock never advances


def test_rate_limit_retries_honor_retry_after():
    sleeps = []
    api = FakeApi(http_error(429, retry_after="7"), page(1))
    assert client(api, sleeps).get_json("https://api/x") == page(1)
    assert 7.0 in sleeps


def test_server_errors_back_off_exponentially_then_give_up():
    sleeps = []
    api = FakeApi(*[http_error(503)] * 5)
    with pytest.raises(ExtractError, match="Gave up after 5 attempts"):
        client(api, sleeps).get_json("https://api/x")
    assert [s for s in sleeps if s >= 1] == [1.0, 2.0, 4.0, 8.0, 16.0]


def test_client_errors_fail_fast():
    api = FakeApi(http_error(404), page(1))
    with pytest.raises(ExtractError, match="HTTP 404"):
        client(api).get_json("https://api/x")
    assert len(api.urls) == 1


def test_page_key_layout():
    assert Page("sprint", 2024, 1, {}).key == "sprint/2024/page-001.json"


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("2021-2023", [2021, 2022, 2023]),
        ("2025,2023", [2023, 2025]),
        ("current", [2026]),
        ("2024-current", [2024, 2025, 2026]),
    ],
)
def test_parse_seasons(spec, expected):
    assert parse_seasons(spec, today=date(2026, 10, 28)) == expected


def test_parse_seasons_rejects_backwards_range():
    with pytest.raises(ValueError, match="backwards"):
        parse_seasons("2025-2021")
