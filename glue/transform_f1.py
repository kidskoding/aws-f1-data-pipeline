"""Glue ETL job: turn raw F1 API JSON into clean, typed tables.

Input (raw zone, exactly as the API returned it):
    {raw_path}/results/<season>/page-NNN.json           race results
    {raw_path}/sprint/<season>/page-NNN.json            sprint results
    {raw_path}/driver_standings/<season>/page-NNN.json  official standings

Output (curated zone, Parquet, one folder per table):
    results, races, drivers, constructors, circuits, official_standings
    plus {rejects_path}: result rows that failed a rule, with the reason.

Runs as an AWS Glue 5.0 job (Spark 3.5) or locally with spark-submit.
"""

import argparse
import sys

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

S = T.StringType()


def _struct(*fields: tuple[str, T.DataType]) -> T.StructType:
    return T.StructType([T.StructField(name, dtype) for name, dtype in fields])


# Explicit schema: only the fields we use, all read as strings (the API sends numbers as strings).
# Reading with a fixed schema means a changed API response fails loudly instead of drifting silently.
DRIVER = _struct(
    ("driverId", S),
    ("permanentNumber", S),
    ("code", S),
    ("givenName", S),
    ("familyName", S),
    ("dateOfBirth", S),
    ("nationality", S),
)
CONSTRUCTOR = _struct(("constructorId", S), ("name", S), ("nationality", S))
RESULT = _struct(
    ("number", S),
    ("position", S),
    ("positionText", S),
    ("points", S),
    ("Driver", DRIVER),
    ("Constructor", CONSTRUCTOR),
    ("grid", S),
    ("laps", S),
    ("status", S),
    ("Time", _struct(("millis", S), ("time", S))),
    ("FastestLap", _struct(("rank", S), ("lap", S), ("Time", _struct(("time", S))))),
)
CIRCUIT = _struct(
    ("circuitId", S),
    ("circuitName", S),
    ("Location", _struct(("lat", S), ("long", S), ("locality", S), ("country", S))),
)
RACE = _struct(
    ("season", S),
    ("round", S),
    ("raceName", S),
    ("date", S),
    ("Circuit", CIRCUIT),
    ("Results", T.ArrayType(RESULT)),
    ("SprintResults", T.ArrayType(RESULT)),
)
STANDING = _struct(
    ("position", S), ("points", S), ("wins", S), ("Driver", DRIVER), ("Constructors", T.ArrayType(CONSTRUCTOR))
)
PAGE = _struct(
    (
        "MRData",
        _struct(
            ("total", S),
            ("RaceTable", _struct(("season", S), ("Races", T.ArrayType(RACE)))),
            (
                "StandingsTable",
                _struct(
                    ("season", S),
                    (
                        "StandingsLists",
                        T.ArrayType(_struct(("season", S), ("round", S), ("DriverStandings", T.ArrayType(STANDING)))),
                    ),
                ),
            ),
        ),
    )
)

RESULT_COLUMNS = [
    "session",
    "season",
    "round",
    "driver_id",
    "constructor_id",
    "car_number",
    "grid",
    "position",
    "position_text",
    "classified",
    "points",
    "laps",
    "status",
    "finish_category",
    "race_time_ms",
    "fastest_lap_rank",
    "fastest_lap_time_ms",
]
FINISH_CATEGORIES = ["finished", "dnf", "disqualified", "did_not_start"]


def parse_args(argv: list[str]) -> dict[str, str]:
    keys = ["raw_path", "curated_path", "rejects_path", "catalog_db"]
    try:
        from awsglue.utils import getResolvedOptions  # only exists inside Glue

        opts = getResolvedOptions(argv, keys)
    except ImportError:
        parser = argparse.ArgumentParser()
        for key in keys:
            parser.add_argument(f"--{key}", default="")
        opts = vars(parser.parse_args(argv[1:]))
    return {k: v.rstrip("/") for k, v in opts.items()}


def read_pages(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.schema(PAGE)
        .option("multiLine", True)
        .option("recursiveFileLookup", True)
        .json(path)
        .withColumn("source_file", F.input_file_name())
    )


def lap_time_ms(col: str) -> Column:
    """'1:22.167' or '59.5' (seconds) to integer milliseconds."""
    c = F.trim(F.col(col))
    parts = F.split(c, ":")
    with_minutes = parts.getItem(0).cast("double") * 60000 + parts.getItem(1).cast("double") * 1000
    return F.round(F.when(c.contains(":"), with_minutes).otherwise(c.cast("double") * 1000)).cast("int")


def finish_category(status: Column, position_text: Column) -> Column:
    return (
        F.when((status == "Finished") | (status == "Lapped") | status.rlike(r"^\+[0-9]+ Laps?$"), "finished")
        .when((status == "Disqualified") | position_text.isin("D", "E"), "disqualified")
        .when(status.isin("Did not start", "Withdrew") | position_text.isin("W", "F"), "did_not_start")
        .otherwise("dnf")
    )


def flatten_results(pages: DataFrame, session: str) -> DataFrame:
    """One row per driver per race (or sprint). Values stay raw strings except trimming."""
    array_field = "Results" if session == "race" else "SprintResults"
    races = pages.select(F.explode("MRData.RaceTable.Races").alias("race"), "source_file")
    rows = races.select("race", "source_file", F.explode(f"race.{array_field}").alias("r"))
    return rows.select(
        F.lit(session).alias("session"),
        F.trim("race.season").alias("season"),
        F.trim("race.round").alias("round"),
        F.trim("r.Driver.driverId").alias("driver_id"),
        F.trim("r.Constructor.constructorId").alias("constructor_id"),
        F.col("r.number").alias("car_number"),
        F.col("r.grid").alias("grid"),
        F.col("r.position").alias("position"),
        F.trim("r.positionText").alias("position_text"),
        F.col("r.points").alias("points"),
        F.col("r.laps").alias("laps"),
        F.trim("r.status").alias("status"),
        F.col("r.Time.millis").alias("race_time_ms"),
        F.col("r.FastestLap.rank").alias("fastest_lap_rank"),
        F.col("r.FastestLap.Time.time").alias("fastest_lap_time"),
        "source_file",
    )


def clean_results(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Type the flattened rows. Return (clean, rejected); every rejected row has a reject_reason."""
    numeric = {
        "season": "int",
        "round": "int",
        "car_number": "int",
        "grid": "int",
        "position": "int",
        "points": "decimal(6,2)",
        "laps": "int",
        "race_time_ms": "bigint",
        "fastest_lap_rank": "int",
    }
    typed = raw
    bad_number = F.lit(False)
    for col, dtype in numeric.items():
        typed = typed.withColumn(f"_raw_{col}", F.col(col)).withColumn(col, F.col(col).cast(dtype))
        # A value that was present but didn't convert is bad data, not a missing value.
        bad_number = bad_number | (F.col(f"_raw_{col}").isNotNull() & F.col(col).isNull())
    typed = typed.withColumn("_bad_number", bad_number).withColumn(
        "fastest_lap_time_ms", lap_time_ms("fastest_lap_time")
    )

    # The API pages by result row, so a race can span two pages. Reruns can also overlap.
    # Keep one row per key, chosen deterministically.
    key = ["session", "season", "round", "driver_id"]
    rank = Window.partitionBy(*key).orderBy(F.col("source_file").asc(), F.col("position").asc_nulls_last())
    checked = typed.withColumn("_rank", F.row_number().over(rank)).withColumn(
        "reject_reason",
        F.when(
            F.col("season").isNull()
            | F.col("round").isNull()
            | F.col("driver_id").isNull()
            | (F.col("driver_id") == ""),
            "missing_key",
        )
        .when(F.col("_bad_number"), "bad_number")
        .when(F.col("_rank") > 1, "duplicate_result")
        .when(F.col("points") < 0, "negative_points"),
    )

    clean = (
        checked.where(F.col("reject_reason").isNull())
        .withColumn("classified", F.col("position_text").rlike("^[0-9]+$"))
        .withColumn("finish_category", finish_category(F.col("status"), F.col("position_text")))
        .select(*RESULT_COLUMNS)
    )
    rejected = checked.where(F.col("reject_reason").isNotNull()).select(
        "session",
        *[f"_raw_{c}" for c in ("season", "round")],
        "driver_id",
        "constructor_id",
        "status",
        "reject_reason",
        "source_file",
    )
    return clean, rejected


def latest_per_key(df: DataFrame, key: str) -> DataFrame:
    """Keep each entity's most recent version (e.g. a driver's current permanent number)."""
    rank = Window.partitionBy(key).orderBy(F.col("_season").desc(), F.col("_round").desc())
    return df.withColumn("_rank", F.row_number().over(rank)).where("_rank = 1").drop("_rank", "_season", "_round")


def build_dimensions(result_pages: DataFrame, sprint_pages: DataFrame) -> dict[str, DataFrame]:
    races = (
        result_pages.unionByName(sprint_pages)
        .select(F.explode("MRData.RaceTable.Races").alias("race"))
        .select("race.*")
    )
    when = [F.col("season").cast("int").alias("_season"), F.col("round").cast("int").alias("_round")]
    entries = races.select(*when, F.explode("Results").alias("r")).unionByName(
        races.select(*when, F.explode("SprintResults").alias("r"))
    )

    drivers = latest_per_key(
        entries.select(
            "_season",
            "_round",
            F.col("r.Driver.driverId").alias("driver_id"),
            F.col("r.Driver.code").alias("code"),
            F.col("r.Driver.permanentNumber").cast("int").alias("permanent_number"),
            F.col("r.Driver.givenName").alias("given_name"),
            F.col("r.Driver.familyName").alias("family_name"),
            F.to_date("r.Driver.dateOfBirth").alias("date_of_birth"),
            F.col("r.Driver.nationality").alias("nationality"),
        ),
        "driver_id",
    )
    constructors = latest_per_key(
        entries.select(
            "_season",
            "_round",
            F.col("r.Constructor.constructorId").alias("constructor_id"),
            F.col("r.Constructor.name").alias("name"),
            F.col("r.Constructor.nationality").alias("nationality"),
        ),
        "constructor_id",
    )
    circuits = latest_per_key(
        races.select(
            F.col("season").cast("int").alias("_season"),
            F.col("round").cast("int").alias("_round"),
            F.col("Circuit.circuitId").alias("circuit_id"),
            F.col("Circuit.circuitName").alias("circuit_name"),
            F.col("Circuit.Location.locality").alias("locality"),
            F.col("Circuit.Location.country").alias("country"),
            F.col("Circuit.Location.lat").cast("double").alias("lat"),
            F.col("Circuit.Location.long").cast("double").alias("lng"),
        ),
        "circuit_id",
    )
    race_table = races.select(
        F.col("season").cast("int").alias("season"),
        F.col("round").cast("int").alias("round"),
        F.col("raceName").alias("race_name"),
        F.to_date("date").alias("race_date"),
        F.col("Circuit.circuitId").alias("circuit_id"),
    ).dropDuplicates(["season", "round"])
    return {"drivers": drivers, "constructors": constructors, "circuits": circuits, "races": race_table}


def build_official_standings(standing_pages: DataFrame) -> DataFrame:
    lists = standing_pages.select(F.explode("MRData.StandingsTable.StandingsLists").alias("l"))
    return (
        lists.select(
            F.col("l.season").cast("int").alias("season"),
            F.col("l.round").cast("int").alias("round"),
            F.explode("l.DriverStandings").alias("s"),
        )
        .select(
            "season",
            "round",
            F.col("s.Driver.driverId").alias("driver_id"),
            F.element_at("s.Constructors.constructorId", -1).alias("constructor_id"),  # team at season end
            F.col("s.position").cast("int").alias("position"),
            F.col("s.points").cast("decimal(6,2)").alias("points"),
            F.col("s.wins").cast("int").alias("wins"),
        )
        .dropDuplicates(["season", "driver_id"])
    )


DATA_QUALITY_RULES = """Rules = [
    RowCount > 0,
    IsComplete "driver_id",
    IsComplete "constructor_id",
    IsPrimaryKey "session" "season" "round" "driver_id",
    ColumnValues "points" >= 0,
    ColumnValues "finish_category" in ["finished", "dnf", "disqualified", "did_not_start"],
    ColumnValues "session" in ["race", "sprint"]
]"""


def run_data_quality(glue_context, df: DataFrame, name: str) -> None:
    """Glue Data Quality: evaluate DQDL rules, publish results to the Glue console and CloudWatch."""
    from awsglue.dynamicframe import DynamicFrame
    from awsgluedq.transforms import EvaluateDataQuality

    outcome = EvaluateDataQuality.apply(
        frame=DynamicFrame.fromDF(df, glue_context, name),
        ruleset=DATA_QUALITY_RULES,
        publishing_options={
            "dataQualityEvaluationContext": name,
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
    ).toDF()
    for row in outcome.collect():
        print(f"  data quality {row['Outcome']}: {row['Rule']}")
    if outcome.where("Outcome != 'Passed'").count():
        raise RuntimeError("Glue Data Quality rules failed; curated data was not published")


def hive_type(dtype: T.DataType) -> str:
    """Spark type to the Hive type name the Glue Data Catalog, Athena and Spectrum expect."""
    if isinstance(dtype, T.DecimalType):
        return f"decimal({dtype.precision},{dtype.scale})"
    names = {
        T.StringType: "string",
        T.IntegerType: "int",
        T.LongType: "bigint",
        T.DoubleType: "double",
        T.BooleanType: "boolean",
        T.DateType: "date",
        T.TimestampType: "timestamp",
    }
    for spark_type, name in names.items():
        if isinstance(dtype, spark_type):
            return name
    raise TypeError(f"No catalog type mapping for {dtype}")


def catalog_table_input(table: str, path: str, schema: T.StructType) -> dict[str, object]:
    """A plain Hive-style Parquet table definition, readable by Athena and Redshift Spectrum."""
    return {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "StorageDescriptor": {
            "Columns": [{"Name": f.name, "Type": hive_type(f.dataType)} for f in schema.fields],
            "Location": path.rstrip("/") + "/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
        },
    }


def register_table(glue_client, database: str, table: str, path: str, schema: T.StructType) -> None:
    table_input = catalog_table_input(table, path, schema)
    try:
        glue_client.update_table(DatabaseName=database, TableInput=table_input)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=database, TableInput=table_input)


def write(df: DataFrame, path: str, glue_client, catalog_db: str, table: str) -> None:
    # coalesce(1): the data is small, and one file per table keeps S3 easy to browse.
    df.coalesce(1).write.mode("overwrite").parquet(path)
    if catalog_db and glue_client is not None:
        register_table(glue_client, catalog_db, table, path, df.schema)


def main() -> None:
    opts = parse_args(sys.argv)
    spark = SparkSession.builder.appName("transform-f1").getOrCreate()
    # Redshift COPY reads every object under a prefix, so skip Spark's empty _SUCCESS marker file.
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    try:
        import boto3
        from awsglue.context import GlueContext

        glue_context = GlueContext(spark.sparkContext)
        glue_client = boto3.client("glue")
    except ImportError:
        # Running locally: Glue Data Quality and the Data Catalog only exist inside AWS.
        glue_context, glue_client = None, None

    raw = opts["raw_path"]
    result_pages = read_pages(spark, f"{raw}/results").cache()
    sprint_pages = read_pages(spark, f"{raw}/sprint").cache()
    standing_pages = read_pages(spark, f"{raw}/driver_standings")

    raw_rows = flatten_results(result_pages, "race").unionByName(flatten_results(sprint_pages, "sprint")).cache()
    results, rejected = clean_results(raw_rows)
    results, rejected = results.cache(), rejected.cache()

    n_raw, n_clean, n_rejected = raw_rows.count(), results.count(), rejected.count()
    print(f"result rows: raw={n_raw} clean={n_clean} rejected={n_rejected}")
    for row in rejected.groupBy("reject_reason").count().orderBy("reject_reason").collect():
        print(f"  rejected {row['reject_reason']}: {row['count']}")
    if n_raw != n_clean + n_rejected:
        raise RuntimeError("Row counts don't reconcile: raw != clean + rejected")
    if glue_context is not None:
        run_data_quality(glue_context, results, "f1_results")

    curated, db = opts["curated_path"], opts["catalog_db"]
    tables = {
        "results": results,
        **build_dimensions(result_pages, sprint_pages),
        "official_standings": build_official_standings(standing_pages),
    }
    for name, df in tables.items():
        write(df, f"{curated}/{name}", glue_client, db, name)
        print(f"  wrote {name}")
    write(rejected, opts["rejects_path"], glue_client, db, "rejected_results")
    spark.stop()


if __name__ == "__main__":
    main()
