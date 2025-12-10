# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "sqlspec[adbc]",
#     "pyarrow",
#     "rich",
#     "rich-click",
# ]
# ///
"""ADBC Postgres ingestion workflow leveraging the storage bridge.

This example exports arbitrary SELECT statements to a Parquet or Arrow artifact,
then loads the staged data back into a target table using the same ADBC driver.
Use it as a template for warehouse ↔ object-store fan-outs.
"""

from pathlib import Path
from typing import Any

import rich_click as click
from rich.console import Console
from rich.table import Table

from sqlspec import SQLSpec
from sqlspec.adapters.adbc import AdbcConfig
from sqlspec.storage import StorageTelemetry
from sqlspec.utils.serializers import to_json

__all__ = ("main",)


def _build_partitioner(rows_per_chunk: int | None, partitions: int | None) -> "dict[str, Any] | None":
    if rows_per_chunk and partitions:
        msg = "Use either --rows-per-chunk or --partitions, not both."
        raise click.BadParameter(msg, param_hint="--rows-per-chunk / --partitions")
    if rows_per_chunk:
        return {"kind": "rows_per_chunk", "rows_per_chunk": rows_per_chunk}
    if partitions:
        return {"kind": "fixed", "partitions": partitions}
    return None


def _write_telemetry(payload: "dict[str, Any]", output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(to_json(payload), encoding="utf-8")


def _format_job(stage: str, telemetry: StorageTelemetry) -> str:
    rows = telemetry.get("rows_processed", 0)
    bytes_processed = telemetry.get("bytes_processed", 0)
    destination = telemetry.get("destination", "")
    return f"[{stage}] rows={rows} bytes={bytes_processed} destination={destination}"


def _render_capabilities(console: Console, config: AdbcConfig) -> None:
    capabilities = config.storage_capabilities()
    table = Table(title="Storage Capabilities", highlight=True)
    table.add_column("Capability", style="cyan")
    table.add_column("Enabled", style="green")
    for key, value in capabilities.items():
        table.add_row(str(key), str(value))
    console.print(table)


@click.command(context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 100})
@click.option(
    "--uri",
    required=True,
    envvar="SQLSPEC_ADBC_URI",
    help="ADBC connection URI (e.g. postgres://user:pass@host:port/dbname)",
)
@click.option("--source-sql", required=True, help="SELECT statement to export")
@click.option("--target-table", required=True, help="Fully qualified destination table name")
@click.option(
    "--destination",
    type=click.Path(path_type=Path, dir_okay=False, writable=True, resolve_path=True),
    default=Path("./tmp/adbc_export.parquet"),
    show_default=True,
    help="Local path or mounted volume for the staged artifact",
)
@click.option(
    "--format",
    "file_format",
    type=click.Choice(["parquet", "arrow-ipc"], case_sensitive=False),
    default="parquet",
    show_default=True,
    help="Storage format used for export/import",
)
@click.option(
    "--rows-per-chunk",
    type=int,
    help="Rows per partition chunk. Combine with SQL predicates (e.g. `WHERE id BETWEEN ...`) per worker.",
)
@click.option(
    "--partitions",
    type=int,
    help="Fixed number of partitions. Pair with predicates like `MOD(id, N) = worker_id` when parallelizing.",
)
@click.option(
    "--overwrite/--no-overwrite", default=False, show_default=True, help="Overwrite the target table before load"
)
@click.option("--skip-load", is_flag=True, default=False, help="Export only and skip the load stage")
@click.option(
    "--output-telemetry",
    type=click.Path(path_type=Path, dir_okay=False, writable=True, resolve_path=True),
    help="Optional path to persist telemetry JSON",
)
@click.version_option(message="%(version)s")
def main(
    *,
    uri: str,
    source_sql: str,
    target_table: str,
    destination: Path,
    file_format: str,
    rows_per_chunk: int | None,
    partitions: int | None,
    overwrite: bool,
    skip_load: bool,
    output_telemetry: Path | None,
) -> None:
    """ADBC-powered export/import demo for Postgres-compatible backends."""

    console = Console()
    partitioner = _build_partitioner(rows_per_chunk, partitions)
    destination.parent.mkdir(parents=True, exist_ok=True)

    db_manager = SQLSpec()
    adbc_config = db_manager.add_config(AdbcConfig(connection_config={"uri": uri}))

    _render_capabilities(console, adbc_config)
    telemetry_records: list[dict[str, Any]] = []

    with db_manager.provide_session(adbc_config) as session:
        export_job = session.select_to_storage(
            source_sql, str(destination), format_hint=file_format, partitioner=partitioner
        )
        console.print(_format_job("export", export_job.telemetry))
        telemetry_records.append({"stage": "export", "metrics": export_job.telemetry})

        if not skip_load:
            load_job = session.load_from_storage(
                target_table, str(destination), file_format=file_format, overwrite=overwrite, partitioner=partitioner
            )
            console.print(_format_job("load", load_job.telemetry))
            telemetry_records.append({"stage": "load", "metrics": load_job.telemetry})

        if partitioner:
            console.print(
                "[dim]Tip:[/] launch multiple workers with mutually exclusive WHERE clauses ("
                "for example, `MOD(id, N) = worker_id`) so each process writes a distinct partition."
            )

    if output_telemetry:
        payload: dict[str, Any] = {"telemetry": telemetry_records}
        _write_telemetry(payload, output_telemetry)


if __name__ == "__main__":
    main()
