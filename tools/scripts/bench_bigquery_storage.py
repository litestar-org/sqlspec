"""Compare BigQuery export routes with explicit endpoints and honest measurements.

Run as ``uv run python -m tools.scripts.bench_bigquery_storage --help``.
Cloud latency and crossover have not been verified. Local emulator results are
local-only; an unavailable native route is reported as unsupported, never timed
as a successful native export. Exported objects are retained for inspection.
"""

import argparse
import io
import json
import sys
import tempfile
from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import urlparse

from google.auth.credentials import AnonymousCredentials
from google.cloud.bigquery import Client

from sqlspec.adapters.bigquery.driver import BigQueryDriver
from sqlspec.storage import StorageFormat

__all__ = ("characterize_ingest", "main", "run_benchmark")

SCENARIO = "bigquery_storage_export"
QUERY = "SELECT value FROM UNNEST(GENERATE_ARRAY(1, :row_count)) AS value"


def characterize_ingest(
    sizes: Sequence[int] = (100, 1000, 10000), *, consume: Callable[[str, bytes | Path], None] | None = None
) -> list[dict[str, Any]]:
    """Measure local encoding; model both paths' client upload responsibility.

    Optional consumers are local test doubles, not BigQuery or GCS clients.
    No service transfer/job latency is measured. Temporary landing files are
    removed after consumers return, including when a consumer raises.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not sizes or any(size <= 0 for size in sizes):
        raise ValueError("Sizes must be positive")
    records: list[dict[str, Any]] = []
    for size in sizes:
        table = pa.table({"value": range(size)})
        buffer = io.BytesIO()
        started = perf_counter()
        pq.write_table(table, buffer)
        payload = buffer.getvalue()
        encoding_s = perf_counter() - started
        with tempfile.TemporaryDirectory(prefix="sqlspec-bigquery-ingest-") as directory:
            landing = Path(directory) / "landing.parquet"
            started = perf_counter()
            landing.write_bytes(payload)
            local_write_s = perf_counter() - started
            if consume is not None:
                consume("direct-file", payload)
                consume("landing-plus-uri", landing)
            records.append({
                "scenario": "bigquery_arrow_ingest_characterization",
                "environment": "local-codec-filesystem",
                "input_rows": size,
                "encoded_bytes": len(payload),
                "encoding_s": encoding_s,
                "local_landing_write_s": local_write_s,
                "direct_client_upload_bytes": len(payload),
                "landing_client_upload_bytes": landing.stat().st_size,
                "upload_accounting": "payload responsibility; no network transfer measured",
                "landing_requires": ["storage configuration", "object cleanup", "URI load job"],
                "cloud_performance_verified": False,
            })
    return records


def run_benchmark(
    driver_factory: Callable[[bool], AbstractContextManager[BigQueryDriver]],
    *,
    destination: str,
    environment: str,
    sizes: Sequence[int] = (100, 1000, 10000),
    warmup: int = 1,
    repetitions: int = 3,
    format_hint: StorageFormat = "parquet",
) -> list[dict[str, Any]]:
    """Measure complete export calls; no submission-only timer is available."""
    if not sizes or any(size <= 0 for size in sizes) or warmup < 0 or repetitions <= 0:
        raise ValueError("Sizes and repetitions must be positive; warmup cannot be negative")
    records: list[dict[str, Any]] = []
    for native in (True, False):
        route = "native" if native else "client"
        with driver_factory(native) as driver:
            for size in sizes:
                for iteration in range(-warmup, repetitions):
                    record: dict[str, Any] = {
                        "scenario": SCENARIO,
                        "environment": environment,
                        "route": route,
                        "input_rows": size,
                        "format": format_hint,
                        "iteration": iteration,
                        "cloud_performance_verified": False,
                    }
                    target = f"{destination.rstrip('/')}/{route}/{size}/{iteration}/result.{format_hint}"
                    started = perf_counter()
                    try:
                        job = driver.select_to_storage(QUERY, target, {"row_count": size}, format_hint=format_hint)
                        elapsed = perf_counter() - started
                        telemetry = job.telemetry
                        if native != bool(telemetry.get("extra", {}).get("native_export", False)):
                            record.update(status="unsupported", reason="requested export route was not used")
                        elif not isinstance(telemetry.get("destination"), str):
                            record.update(status="failed", reason="missing export destination telemetry")
                        else:
                            record.update(status="ok", total_s=elapsed, destination=telemetry["destination"])
                            output_bytes = telemetry.get("bytes_processed")
                            if output_bytes is not None:
                                if (
                                    isinstance(output_bytes, bool)
                                    or not isinstance(output_bytes, int)
                                    or output_bytes < 0
                                ):
                                    record.update(status="failed", reason="invalid output byte measurement")
                                else:
                                    record.update(output_bytes=output_bytes, bytes_source="storage_telemetry")
                    except Exception as exc:
                        record.update(status="failed", reason=f"{type(exc).__name__}: {exc}")
                    if iteration >= 0 or record["status"] != "ok":
                        records.append(record)
                    if record["status"] != "ok":
                        break
    return records


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["export", "ingest-local"], default="export")
    parser.add_argument("--project")
    parser.add_argument("--endpoint", help="Explicit BigQuery API endpoint; no implicit cloud target")
    parser.add_argument("--destination", help="Output prefix; artifacts are retained")
    parser.add_argument("--sizes", nargs="+", type=int, default=[100, 1000, 10000])
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args(argv)
    if args.mode == "ingest-local":
        json.dump(characterize_ingest(args.sizes), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    if not args.project or not args.endpoint or not args.destination:
        parser.error("export requires --project, --endpoint and --destination")
    local = urlparse(args.endpoint).hostname in {"localhost", "127.0.0.1", "::1"}

    @contextmanager
    def factory(native: bool) -> Iterator[BigQueryDriver]:
        with Client(
            project=args.project,
            client_options={"api_endpoint": args.endpoint},
            credentials=AnonymousCredentials() if local else None,
        ) as client:
            yield BigQueryDriver(
                client,
                driver_features={
                    "enable_native_storage": native,
                    "storage_capabilities": {"arrow_export_enabled": True},
                },
            )

    records = run_benchmark(
        factory,
        destination=args.destination,
        environment="local-emulator" if local else "explicit-provider",
        sizes=args.sizes,
        warmup=args.warmup,
        repetitions=args.repetitions,
    )
    json.dump(records, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return int(any(record["status"] == "failed" for record in records))


if __name__ == "__main__":
    raise SystemExit(main())
