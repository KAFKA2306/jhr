from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

from fixed_yaml_generator import AuditedJHRDataExtractor

MAX_FILE_BYTES = 50 * 1024 * 1024
ALLOWED_HOSTS = {"www.jhrth.co.jp", "jhrth.co.jp"}
XLSX_MAGIC = b"PK\x03\x04"
XLS_MAGIC = bytes.fromhex("D0CF11E0A1B11AE1")


class DownloadError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
        raise DownloadError(
            "Only explicit HTTPS URLs on the official jhrth.co.jp domain are accepted"
        )
    if parsed.username or parsed.password:
        raise DownloadError("Credential-bearing URLs are not accepted")
    return url


def download_excel(
    *,
    year: int,
    url: str,
    data_dir: str | Path = "data",
    expected_sha256: str | None = None,
    force: bool = False,
    session: requests.Session | None = None,
) -> dict[str, object]:
    if not 2000 <= year <= 2200:
        raise ValueError("year is outside the supported range")
    url = _validate_url(url)
    directory = Path(data_dir)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"jhr_{year}_hotel_performance.xlsx"

    if destination.exists() and not force:
        actual = _sha256(destination)
        if expected_sha256 and actual.lower() != expected_sha256.lower():
            raise DownloadError(
                f"Existing file hash mismatch: expected={expected_sha256}, actual={actual}"
            )
        return {
            "year": year,
            "url": url,
            "path": str(destination),
            "sha256": actual,
            "bytes": destination.stat().st_size,
            "downloaded": False,
        }

    response = (session or requests.Session()).get(url, timeout=60, stream=True)
    response.raise_for_status()
    declared = response.headers.get("Content-Length")
    if declared and int(declared) > MAX_FILE_BYTES:
        raise DownloadError("Excel response exceeds size limit")

    with tempfile.NamedTemporaryFile(dir=directory, delete=False) as temporary:
        temp_path = Path(temporary.name)
        total = 0
        for chunk in response.iter_content(64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_FILE_BYTES:
                temporary.close()
                temp_path.unlink(missing_ok=True)
                raise DownloadError("Excel download exceeded size limit")
            temporary.write(chunk)

    try:
        header = temp_path.read_bytes()[:8]
        if not (header.startswith(XLSX_MAGIC) or header.startswith(XLS_MAGIC)):
            raise DownloadError("Downloaded response is not an XLSX/XLS container")
        actual_sha = _sha256(temp_path)
        if expected_sha256 and actual_sha.lower() != expected_sha256.lower():
            raise DownloadError(
                f"Downloaded hash mismatch: expected={expected_sha256}, actual={actual_sha}"
            )
        os.replace(temp_path, destination)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise

    record = {
        "year": year,
        "url": url,
        "path": str(destination),
        "sha256": actual_sha,
        "bytes": destination.stat().st_size,
        "downloaded": True,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "content_type": response.headers.get("Content-Type"),
    }
    manifest_path = directory / "source_manifest.json"
    existing = []
    if manifest_path.exists():
        loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(loaded, list):
            existing = [item for item in loaded if item.get("year") != year]
    existing.append(record)
    manifest_path.write_text(
        json.dumps(sorted(existing, key=lambda item: item["year"]), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return record


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download one explicitly identified JHR Excel file. Automatic year/URL "
            "inference from nearby HTML text is intentionally disabled."
        )
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--expected-sha256")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--extract", action="store_true")
    args = parser.parse_args()

    record = download_excel(
        year=args.year,
        url=args.url,
        data_dir=args.data_dir,
        expected_sha256=args.expected_sha256,
        force=args.force,
    )
    print(json.dumps(record, ensure_ascii=False, indent=2))
    if args.extract:
        extractor = AuditedJHRDataExtractor(args.data_dir)
        result = extractor.process_excel_file(args.year)
        output = Path(f"jhr_{args.year}_audited.yaml")
        output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print(output)


if __name__ == "__main__":
    main()
