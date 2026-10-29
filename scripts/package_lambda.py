"""Build build/lambda.zip from src/f1_pipeline, reproducibly.

Fixed timestamps and sorted entries mean the same code always gives the same
zip, so Terraform only redeploys Lambda when the code actually changed.
"""

import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src"
OUTPUT = ROOT / "build" / "lambda.zip"
FIXED_TIME = (2020, 1, 1, 0, 0, 0)


def main() -> int:
    files = sorted(
        p
        for p in (SOURCE / "f1_pipeline").rglob("*")
        if p.is_file() and "__pycache__" not in p.parts and p.suffix in {".py", ".sql"}
    )
    OUTPUT.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(OUTPUT, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            info = zipfile.ZipInfo(path.relative_to(SOURCE).as_posix(), FIXED_TIME)
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, path.read_bytes())
    print(f"{OUTPUT.relative_to(ROOT)}: {len(files)} files, {OUTPUT.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
