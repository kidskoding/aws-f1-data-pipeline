"""Local extract: `python -m f1_pipeline.extract --seasons 2021-current --out data/raw`."""

from __future__ import annotations

import argparse
import json

from f1_pipeline.extract import extract
from f1_pipeline.extract.jolpica import JolpicaClient, parse_seasons
from f1_pipeline.extract.sinks import LocalSink
from f1_pipeline.logs import configure_logging


def main() -> None:
    parser = argparse.ArgumentParser(description="Copy F1 data from the Jolpica API into a local raw folder.")
    parser.add_argument("--seasons", default="2021-current", help="e.g. 2021-current, 2025, 2023,2025")
    parser.add_argument("--out", default="data/raw", help="local raw zone folder")
    args = parser.parse_args()
    configure_logging()
    summary = extract(JolpicaClient(), LocalSink(args.out), parse_seasons(args.seasons))
    print(json.dumps(summary.as_dict(), indent=2))


if __name__ == "__main__":
    main()
