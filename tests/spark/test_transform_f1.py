"""Tests for glue/transform_f1.py, using small hand-made pages shaped like the real API.

Run with `make test-spark` (Spark 3.5 in Docker, the same version as Glue 5.0) or pytest with pyspark installed.
"""

import json
import tempfile
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

from transform_f1 import (
    build_dimensions,
    build_official_standings,
    catalog_table_input,
    clean_results,
    finish_category,
    flatten_results,
    lap_time_ms,
    read_pages,
)


def result(
    driver, position="1", text=None, points="25", status="Finished", grid="1", number="4", team="mclaren", permanent="4"
):
    return {
        "number": number,
        "position": position,
        "positionText": text or position,
        "points": points,
        "Driver": {
            "driverId": driver,
            "permanentNumber": permanent,
            "code": driver[:3].upper(),
            "givenName": "Given",
            "familyName": driver.title(),
            "dateOfBirth": "1999-11-13",
            "nationality": "British",
        },
        "Constructor": {"constructorId": team, "name": team.title(), "nationality": "British"},
        "grid": grid,
        "laps": "57",
        "status": status,
        "FastestLap": {"rank": "1", "lap": "43", "Time": {"time": "1:22.167"}},
    }


def race_page(season, rnd, results, field="Results"):
    return {
        "MRData": {
            "total": "99",
            "RaceTable": {
                "season": season,
                "Races": [
                    {
                        "season": season,
                        "round": rnd,
                        "raceName": "Test Grand Prix",
                        "date": f"{season}-03-16",
                        "Circuit": {
                            "circuitId": "albert_park",
                            "circuitName": "Albert Park",
                            "Location": {
                                "lat": "-37.8",
                                "long": "144.9",
                                "locality": "Melbourne",
                                "country": "Australia",
                            },
                        },
                        field: results,
                    }
                ],
            },
        }
    }


def pages_df(spark, *pages):
    folder = Path(tempfile.mkdtemp())
    for i, page in enumerate(pages):
        (folder / f"page-{i:03d}.json").write_text(json.dumps(page))
    return read_pages(spark, str(folder))


def clean(spark, *pages, session="race"):
    good, bad = clean_results(flatten_results(pages_df(spark, *pages), session))
    return good.collect(), {(r["driver_id"], r["reject_reason"]) for r in bad.collect()}


def test_lap_times_convert_to_milliseconds(spark):
    df = spark.createDataFrame([("1:22.167",), ("59.5",), (None,)], ["t"])
    assert [r[0] for r in df.select(lap_time_ms("t")).collect()] == [82167, 59500, None]


def test_finish_categories(spark):
    rows = [
        ("Finished", "1"),
        ("+2 Laps", "12"),
        ("Lapped", "15"),
        ("Engine", "R"),
        ("Disqualified", "D"),
        ("Did not start", "W"),
        ("Withdrew", "W"),
    ]
    df = spark.createDataFrame(rows, ["status", "text"])
    got = [r[0] for r in df.select(finish_category(F.col("status"), F.col("text"))).collect()]
    assert got == ["finished", "finished", "finished", "dnf", "disqualified", "did_not_start", "did_not_start"]


def test_race_split_across_pages_is_kept_but_true_duplicates_are_rejected(spark):
    page0 = race_page("2025", "1", [result("norris", "1"), result("piastri", "2")])
    page1 = race_page("2025", "1", [result("verstappen", "3"), result("norris", "1")])
    good, bad = clean(spark, page0, page1)
    assert sorted(r["driver_id"] for r in good) == ["norris", "piastri", "verstappen"]
    assert bad == {("norris", "duplicate_result")}


def test_types_and_fields(spark):
    good, bad = clean(spark, race_page("2021", "12", [result("hamilton", points="12.5", number="44")]))
    row = good[0]
    assert bad == set()
    assert (row["season"], row["round"], row["car_number"]) == (2021, 12, 44)
    assert row["points"] == Decimal("12.50")
    assert row["fastest_lap_time_ms"] == 82167
    assert row["classified"] is True and row["finish_category"] == "finished"


def test_retired_driver_is_not_classified(spark):
    good, _ = clean(spark, race_page("2025", "1", [result("sainz", "18", text="R", points="0", status="Engine")]))
    assert good[0]["classified"] is False and good[0]["finish_category"] == "dnf"


def test_sprint_rows_are_tagged(spark):
    good, _ = clean(
        spark, race_page("2025", "6", [result("piastri", points="8")], field="SprintResults"), session="sprint"
    )
    assert good[0]["session"] == "sprint" and good[0]["points"] == Decimal("8.00")


def test_unconvertible_number_is_rejected(spark):
    good, bad = clean(spark, race_page("2025", "1", [result("albon", grid="pit lane")]))
    assert good == [] and bad == {("albon", "bad_number")}


def test_driver_dimension_keeps_latest_version(spark):
    old = race_page("2025", "1", [result("norris", permanent="4")])
    new = race_page("2026", "1", [result("norris", permanent="1")])
    empty = pages_df(spark, race_page("2025", "1", [], field="SprintResults"))
    drivers = build_dimensions(pages_df(spark, old, new), empty)["drivers"].collect()
    assert [(d["driver_id"], d["permanent_number"]) for d in drivers] == [("norris", 1)]


def test_official_standings_use_team_at_season_end(spark):
    page = {
        "MRData": {
            "total": "1",
            "StandingsTable": {
                "season": "2024",
                "StandingsLists": [
                    {
                        "season": "2024",
                        "round": "24",
                        "DriverStandings": [
                            {
                                "position": "10",
                                "points": "26",
                                "wins": "0",
                                "Driver": result("lawson")["Driver"],
                                "Constructors": [{"constructorId": "alphatauri"}, {"constructorId": "rb"}],
                            }
                        ],
                    }
                ],
            },
        }
    }
    row = build_official_standings(pages_df(spark, page)).collect()[0]
    assert (row["driver_id"], row["constructor_id"], row["points"]) == ("lawson", "rb", Decimal("26.00"))


def test_catalog_definition_matches_the_parquet_schema(spark):
    good, _ = clean_results(flatten_results(pages_df(spark, race_page("2025", "1", [result("norris")])), "race"))
    table = catalog_table_input("results", "s3://lake/curated/results", good.schema)
    columns = {c["Name"]: c["Type"] for c in table["StorageDescriptor"]["Columns"]}
    assert list(columns)[:4] == ["session", "season", "round", "driver_id"]
    assert (columns["points"], columns["race_time_ms"], columns["classified"]) == ("decimal(6,2)", "bigint", "boolean")
    assert table["StorageDescriptor"]["Location"] == "s3://lake/curated/results/"
