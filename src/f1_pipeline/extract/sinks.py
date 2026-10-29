"""Where raw pages are written: a local folder for development, or S3 in production."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Protocol


class Sink(Protocol):
    def replace(self, prefix: str) -> None:
        """Remove everything under `prefix`, so a rerun never mixes old and new pages."""

    def put(self, key: str, data: bytes) -> None: ...


class LocalSink:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def replace(self, prefix: str) -> None:
        shutil.rmtree(self.root / prefix, ignore_errors=True)

    def put(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


class S3Sink:
    def __init__(self, s3_client: Any, bucket: str, root_prefix: str = "raw") -> None:
        self.s3 = s3_client
        self.bucket = bucket
        self.root = root_prefix.strip("/")

    def _key(self, key: str) -> str:
        return f"{self.root}/{key}" if self.root else key

    def replace(self, prefix: str) -> None:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(prefix.rstrip("/") + "/")):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": objects, "Quiet": True})

    def put(self, key: str, data: bytes) -> None:
        self.s3.put_object(Bucket=self.bucket, Key=self._key(key), Body=data, ContentType="application/json")
