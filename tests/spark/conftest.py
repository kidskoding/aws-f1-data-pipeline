import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "glue"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
