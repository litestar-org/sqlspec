# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "sqlspec[adbc]",
#     "pyarrow",
#     "rich",
# ]
# ///
"""Reference ingestion workflow for the ADBC storage bridge."""

import argparse
from pathlib import Path
from typing import Any

from rich.console import Console

from sqlspec import SQLSpec
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.storage import StorageTelemetry
from sqlspec.utils.serializers import to_json

__all__ = ("main",)


def _build_partitioner(args: argparse.Namespace) -> "dict[str, Any] | None":
    if args.rows_per_chunk:
        return {"kind": "rows_per_chunk", "rows_per_chunk": args.rows_per_chunk}
    if args.partitions:
        return {"kind": "fixed", "partitions": args.partitions}
    return None


def _write_telemetry(payload: "dict[str, Any]", output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(to_json(payload), encoding="utf-8")


def _format_job(stage: str, telemetry: StorageTelemetry) -> str:
    rows = telemetry.get("rows_processed", 0)
    bytes_processed = telemetry.get("bytes_processed", 0)
    destination = telemetry.get("destination", "")
    return f"[{stage}] rows={rows} bytes={bytes_processed} destination={destination}"


def main() -> None:
    parser = argparse.ArgumentParser(description="ADBC storage bridge demo")
    parser.add_argument("--uri", required=True, help="ADBC connection URI (e.g. postgres://...)")
    parser.add_argument("--source-sql", required=True, help="SELECT statement to export")
    parser.add_argument("--target-table", required=True, help="Destination table name")
    parser.add_argument(
        "--destination", default="./tmp/alloydb_export.parquet", help="Local path for the staged artifact"
    )
    parser.add_argument(
        "--format", choices=["parquet", "arrow-ipc"], default="parquet", help="Storage format for the staged artifact"
    )
    parser.add_argument("--rows-per-chunk", type=int, help="Rows per partition chunk")
    parser.add_argument("--partitions", type=int, help="Fixed partition count")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite destination table before load")
    parser.add_argument("--skip-load", action="store_true", help="Export only and skip the load step")
    parser.add_argument("--output-telemetry", type=Path, help="Optional path to write telemetry JSON")
    args = parser.parse_args()

    console = Console()
    destination_path = Path(args.destination).expanduser().resolve()
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    partitioner = _build_partitioner(args)

    sql = SQLSpec()
    config = AdbcConfig(connection_config={"uri": args.uri})
    sql.add_config(config)

    console.print("[bold green]Storage capabilities[/bold green]", config.storage_capabilities())

    telemetry_records: list[dict[str, Any]] = []

    with sql.provide_session(config) as session:
        export_job = session.select_to_storage(
            args.source_sql, str(destination_path), format_hint=args.format, partitioner=partitioner
        )
        console.print(_format_job("export", export_job.telemetry))
        telemetry_records.append({"stage": "export", "metrics": export_job.telemetry})

        if not args.skip_load:
            load_job = session.load_from_storage(
                args.target_table,
                str(destination_path),
                file_format=args.format,
                overwrite=args.overwrite,
                partitioner=partitioner,
            )
            console.print(_format_job("load", load_job.telemetry))
            telemetry_records.append({"stage": "load", "metrics": load_job.telemetry})

    if args.output_telemetry:
        payload: dict[str, Any] = {"telemetry": telemetry_records}
        _write_telemetry(payload, Path(args.output_telemetry))


if __name__ == "__main__":
    main()
