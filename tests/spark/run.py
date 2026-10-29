"""Run the Spark tests without pytest, for the Spark Docker image (which has no pytest).

`make test-spark` runs this with spark-submit. CI runs the same tests with pytest.
"""

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "glue"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_transform_f1
from pyspark.sql import SparkSession


def main() -> int:
    spark = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    tests = [(name, fn) for name, fn in sorted(vars(test_transform_f1).items()) if name.startswith("test_")]
    failed = 0
    for name, fn in tests:
        try:
            fn(spark)
            print(f"PASS {name}")
        except Exception:
            failed += 1
            print(f"FAIL {name}\n{traceback.format_exc(limit=3)}")
    spark.stop()
    print(f"{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
