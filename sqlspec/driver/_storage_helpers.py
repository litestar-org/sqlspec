"""Pure helper functions for storage operations."""

import asyncio
import contextlib
import multiprocessing
import os
import re
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Final, Literal, Self, cast

from sqlspec.storage import StorageBridgeJob, StorageTelemetry, SyncStoragePipeline, create_storage_bridge_job
from sqlspec.storage._arrow_payload import decode_arrow_payload, encode_arrow_payload
from sqlspec.utils.arrow_helpers import (
    PG_BINARY_COPY_HEADER,
    PG_BINARY_COPY_TRAILER,
    arrow_batch_to_rows,
    arrow_table_needs_parameter_preparation,
    arrow_table_to_rows,
    build_ingest_telemetry,
    coerce_arrow_batches,
    coerce_arrow_table,
    encode_arrow_batch_pg_binary,
    iter_pgcopy_binary_chunks,
    iter_pgcopy_binary_chunks_async,
    records_to_arrow_table,
)
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.storage import StorageDestination, StorageFormat


__all__ = (
    "CAPABILITY_HINTS",
    "PG_BINARY_COPY_HEADER",
    "PG_BINARY_COPY_TRAILER",
    "AsyncWithoutIndexesContext",
    "SyncWithoutIndexesContext",
    "arrow_batch_to_rows",
    "arrow_table_needs_parameter_preparation",
    "arrow_table_to_rows",
    "attach_partition_telemetry",
    "build_checkpoint_insert_sql",
    "build_checkpoint_select_sql",
    "build_checkpoint_table_ddl",
    "build_create_index_sql",
    "build_drop_index_sql",
    "build_drop_stage_table_sql",
    "build_ingest_telemetry",
    "build_stage_merge_sql",
    "build_stage_table_ddl",
    "coerce_arrow_batches",
    "coerce_arrow_table",
    "compute_worker_thread_budget",
    "create_storage_job",
    "encode_arrow_batch_pg_binary",
    "execute_load_from_storage_async",
    "execute_load_from_storage_sync",
    "execute_staged_load_async",
    "execute_staged_load_sync",
    "filter_droppable_table_indexes",
    "ingest_telemetry_from_batches",
    "init_spawn_worker_thread_budget",
    "iter_pgcopy_binary_chunks",
    "iter_pgcopy_binary_chunks_async",
    "records_to_arrow_table",
    "resolve_stage_table_name",
    "stringify_storage_target",
)


CAPABILITY_HINTS: Final[dict[str, str]] = {
    "arrow_export_enabled": "native Arrow export",
    "arrow_import_enabled": "native Arrow import",
    "parquet_export_enabled": "native Parquet export",
    "parquet_import_enabled": "native Parquet import",
}


def stringify_storage_target(target: "StorageDestination | None") -> str | None:
    """Convert storage target to string representation.

    Args:
        target: Storage destination path or None.

    Returns:
        String representation of the path or None.
    """
    if target is None:
        return None
    if isinstance(target, Path):
        return target.as_posix()
    return str(target)


def ingest_telemetry_from_batches(
    rows_processed: int,
    bytes_processed: int,
    *,
    duration_s: float | None = None,
    format_label: str = "arrow",
    read_duration_s: float | None = None,
    encode_duration_s: float | None = None,
    write_duration_s: float | None = None,
    commit_duration_s: float | None = None,
    shards_processed: int | None = None,
    shards_skipped: int | None = None,
    wal_bytes: int | None = None,
) -> "StorageTelemetry":
    """Build a StorageTelemetry payload from accumulated batch metrics.

    Args:
        rows_processed: Total number of rows processed across all batches.
        bytes_processed: Total uncompressed or wire bytes processed across all batches.
        duration_s: Optional total wall-clock duration in seconds.
        format_label: Payload format label (default ``"arrow"``).
        read_duration_s: Optional source read time in seconds.
        encode_duration_s: Optional payload encoding time in seconds.
        write_duration_s: Optional database/storage write time in seconds.
        commit_duration_s: Optional commit/finalization time in seconds.
        shards_processed: Optional count of processed storage shards.
        shards_skipped: Optional count of checkpoint-skipped storage shards.
        wal_bytes: Optional WAL byte delta recorded during ingestion.

    Returns:
        Populated ``StorageTelemetry`` dictionary.
    """
    telemetry: StorageTelemetry = {
        "rows_processed": int(rows_processed),
        "bytes_processed": int(bytes_processed),
        "format": format_label,
    }
    if duration_s is not None:
        telemetry["duration_s"] = float(duration_s)
    if read_duration_s is not None:
        telemetry["read_duration_s"] = float(read_duration_s)
    if encode_duration_s is not None:
        telemetry["encode_duration_s"] = float(encode_duration_s)
    if write_duration_s is not None:
        telemetry["write_duration_s"] = float(write_duration_s)
    if commit_duration_s is not None:
        telemetry["commit_duration_s"] = float(commit_duration_s)
    if shards_processed is not None:
        telemetry["shards_processed"] = int(shards_processed)
    if shards_skipped is not None:
        telemetry["shards_skipped"] = int(shards_skipped)
    if wal_bytes is not None:
        telemetry["wal_bytes"] = int(wal_bytes)
    return telemetry


def attach_partition_telemetry(telemetry: "StorageTelemetry", partitioner: "dict[str, object] | None") -> None:
    """Attach partitioner info to telemetry dict (mutates in place).

    Args:
        telemetry: Telemetry dict to update.
        partitioner: Partitioner configuration or None.
    """
    if not partitioner:
        return
    extra = dict(telemetry.get("extra", {}))
    extra["partitioner"] = partitioner
    telemetry["extra"] = extra


def create_storage_job(
    produced: "StorageTelemetry", provided: "StorageTelemetry | None" = None, *, status: str = "completed"
) -> "StorageBridgeJob":
    """Create a StorageBridgeJob from telemetry data.

    Args:
        produced: Telemetry from the production side of the operation.
        provided: Optional telemetry from the source side.
        status: Job status string.

    Returns:
        StorageBridgeJob instance.
    """
    merged = cast("StorageTelemetry", dict(produced))
    if provided:
        source_bytes = provided.get("bytes_processed")
        if source_bytes is not None:
            merged["bytes_processed"] = int(merged.get("bytes_processed", 0)) + int(source_bytes)
        extra = dict(merged.get("extra", {}))
        extra["source"] = provided
        merged["extra"] = extra
    return create_storage_bridge_job(status, merged)


_PG_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({
    "postgres",
    "postgresql",
    "psycopg",
    "asyncpg",
    "psqlpy",
    "cockroach",
    "cockroachdb",
    "spangres",
})
_ON_CONFLICT_DIALECTS: Final[frozenset[str]] = _PG_FAMILY_DIALECTS | frozenset({"duckdb", "sqlite", "aiosqlite"})
_MYSQL_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({
    "mysql",
    "mariadb",
    "pymysql",
    "aiomysql",
    "asyncmy",
    "mysqlconnector",
})
_MSSQL_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({
    "mssql",
    "tsql",
    "pymssql",
    "mssql_python",
    "sybase",
    "sqlanywhere",
})
_ORACLE_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({"oracle", "oracledb"})
_CREATE_UNIQUE_INDEX_RE: Final = re.compile(r"^\s*CREATE\s+UNIQUE\s+INDEX\s+(?!CONCURRENTLY\b)", re.IGNORECASE)
_CREATE_INDEX_RE: Final = re.compile(r"^\s*CREATE\s+INDEX\s+(?!CONCURRENTLY\b)", re.IGNORECASE)


def compute_worker_thread_budget(max_workers: int, cpu_count: int | None = None) -> "dict[str, str]":
    """Compute per-worker native thread pool environment variables to prevent oversubscription.

    Args:
        max_workers: Number of concurrent ingestion workers.
        cpu_count: Optional logical CPU count override.

    Returns:
        Dictionary of thread-budget environment variables and string values.
    """
    resolved_cpus = cpu_count if cpu_count is not None and cpu_count > 0 else (os.cpu_count() or 1)
    workers = max(1, int(max_workers))
    threads = str(max(1, resolved_cpus // workers))
    return {
        "OMP_NUM_THREADS": threads,
        "ARROW_IO_THREADS": threads,
        "POLARS_MAX_THREADS": threads,
        "RAYON_NUM_THREADS": threads,
        "OPENBLAS_NUM_THREADS": threads,
        "MKL_NUM_THREADS": threads,
    }


def init_spawn_worker_thread_budget(max_workers: int, cpu_count: int | None = None) -> None:
    """Initialize thread-budget environment variables inside a spawned worker process."""
    for key, val in compute_worker_thread_budget(max_workers, cpu_count=cpu_count).items():
        os.environ[key] = val


def build_checkpoint_table_ddl(dialect: str, checkpoint_table: str) -> str:
    """Build DDL to create a multi-shard ingestion checkpoint table if it does not exist."""
    norm = (dialect or "").lower().strip()
    if norm in _MSSQL_FAMILY_DIALECTS:
        return (
            f"IF OBJECT_ID('{checkpoint_table}', 'U') IS NULL "
            f"CREATE TABLE {checkpoint_table} ("
            "phase VARCHAR(128) NOT NULL, "
            "shard_uri VARCHAR(1024) NOT NULL, "
            "rows_loaded BIGINT NOT NULL, "
            "bytes_processed BIGINT NOT NULL, "
            "duration_s FLOAT NOT NULL, "
            "completed_at DATETIME2 DEFAULT CURRENT_TIMESTAMP, "
            "PRIMARY KEY (phase, shard_uri))"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {checkpoint_table} ("
        "phase VARCHAR(128) NOT NULL, "
        "shard_uri VARCHAR(1024) NOT NULL, "
        "rows_loaded BIGINT NOT NULL, "
        "bytes_processed BIGINT NOT NULL, "
        "duration_s DOUBLE PRECISION NOT NULL, "
        "completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        "PRIMARY KEY (phase, shard_uri))"
    )


def build_checkpoint_select_sql(checkpoint_table: str) -> str:
    """Build SQL to select already-completed shard URIs for a given checkpoint phase."""
    return f"SELECT shard_uri FROM {checkpoint_table} WHERE phase = :phase"


def build_checkpoint_insert_sql(dialect: str, checkpoint_table: str) -> str:
    """Build SQL to record a completed shard in the checkpoint table."""
    norm = (dialect or "").lower().strip()
    if norm in _ON_CONFLICT_DIALECTS:
        return (
            f"INSERT INTO {checkpoint_table} (phase, shard_uri, rows_loaded, bytes_processed, duration_s) "
            "VALUES (:phase, :shard_uri, :rows_loaded, :bytes_processed, :duration_s) "
            "ON CONFLICT (phase, shard_uri) DO UPDATE SET "
            "rows_loaded = EXCLUDED.rows_loaded, "
            "bytes_processed = EXCLUDED.bytes_processed, "
            "duration_s = EXCLUDED.duration_s"
        )
    if norm in _MYSQL_FAMILY_DIALECTS:
        return (
            f"INSERT INTO {checkpoint_table} (phase, shard_uri, rows_loaded, bytes_processed, duration_s) "
            "VALUES (:phase, :shard_uri, :rows_loaded, :bytes_processed, :duration_s) "
            "ON DUPLICATE KEY UPDATE "
            "rows_loaded = VALUES(rows_loaded), "
            "bytes_processed = VALUES(bytes_processed), "
            "duration_s = VALUES(duration_s)"
        )
    return (
        f"INSERT INTO {checkpoint_table} (phase, shard_uri, rows_loaded, bytes_processed, duration_s) "
        "VALUES (:phase, :shard_uri, :rows_loaded, :bytes_processed, :duration_s)"
    )


def resolve_stage_table_name(
    target_table: str,
    stage_table: "bool | str | None",
    *,
    conflict_keys: "Sequence[str] | None" = None,
    dialect: str | None = None,
) -> "str | None":
    """Resolve the staging table name when staged bulk loading or conflict resolution is requested."""
    if isinstance(stage_table, str):
        stripped = stage_table.strip()
        return stripped or None
    if stage_table is False:
        return None
    norm_dialect = (dialect or "").lower().strip()
    needs_stage = stage_table is True or (bool(conflict_keys) and norm_dialect not in {"spanner", "cloudspanner"})
    if not needs_stage:
        return None
    suffix = uuid4().hex[:8]
    clean_target = target_table.strip()
    if "." in clean_target:
        schema_part, base_name = clean_target.rsplit(".", 1)
        unquoted_base = base_name.strip('"`[]')
        return f"{schema_part}.{unquoted_base}_stage_{suffix}"
    unquoted_base = clean_target.strip('"`[]')
    return f"{unquoted_base}_stage_{suffix}"


def build_stage_table_ddl(
    dialect: str, target_table: str, stage_table: str, *, multi_worker: bool = False
) -> "list[str]":
    """Build dialect-specific DDL statements to create an empty staging table matching target_table."""
    norm = (dialect or "").lower().strip()
    if norm in _PG_FAMILY_DIALECTS:
        return [f"CREATE UNLOGGED TABLE IF NOT EXISTS {stage_table} (LIKE {target_table} INCLUDING DEFAULTS)"]
    if norm in {"duckdb", "sqlite", "aiosqlite"}:
        if multi_worker or "." in stage_table:
            return [f"CREATE TABLE IF NOT EXISTS {stage_table} AS SELECT * FROM {target_table} WHERE 1=0"]
        return [f"CREATE TEMP TABLE IF NOT EXISTS {stage_table} AS SELECT * FROM {target_table} WHERE 1=0"]
    if norm in _MYSQL_FAMILY_DIALECTS:
        if multi_worker:
            return [f"CREATE TABLE IF NOT EXISTS {stage_table} LIKE {target_table}"]
        return [f"CREATE TEMPORARY TABLE IF NOT EXISTS {stage_table} LIKE {target_table}"]
    if norm in _ORACLE_FAMILY_DIALECTS:
        return [f"CREATE TABLE {stage_table} NOLOGGING AS SELECT * FROM {target_table} WHERE 1=0"]
    if norm in _MSSQL_FAMILY_DIALECTS:
        return [f"SELECT TOP 0 * INTO {stage_table} FROM {target_table}"]
    if norm == "db2":
        return [f"CREATE TABLE {stage_table} AS (SELECT * FROM {target_table}) WITH NO DATA"]
    if norm == "bigquery":
        return [f"CREATE TABLE IF NOT EXISTS {stage_table} LIKE {target_table}"]
    return [f"CREATE TABLE IF NOT EXISTS {stage_table} AS SELECT * FROM {target_table} WHERE 1=0"]


def build_stage_merge_sql(
    dialect: str,
    target_table: str,
    stage_table: str,
    columns: "Sequence[str]",
    *,
    conflict_keys: "Sequence[str] | None" = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: "Sequence[str] | None" = None,
    type_casts: "Mapping[str, str] | None" = None,
) -> str:
    """Build dialect-specific server-side INSERT ... SELECT or MERGE SQL from stage_table into target_table."""
    norm = (dialect or "").lower().strip()
    col_list = ", ".join(columns)
    select_exprs = [
        f"CAST({col} AS {type_casts[col]})" if (type_casts and col in type_casts) else col for col in columns
    ]
    select_list = ", ".join(select_exprs)
    if not conflict_keys:
        return f"INSERT INTO {target_table} ({col_list}) SELECT {select_list} FROM {stage_table}"

    key_list = ", ".join(conflict_keys)
    excluded_set = set(conflict_keys) | set(exclude_update_columns or ())
    update_columns = [col for col in columns if col not in excluded_set]

    if norm in _ON_CONFLICT_DIALECTS:
        if on_conflict == "nothing" or not update_columns:
            return (
                f"INSERT INTO {target_table} ({col_list}) "
                f"SELECT {select_list} FROM {stage_table} WHERE TRUE "
                f"ON CONFLICT ({key_list}) DO NOTHING"
            )
        set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
        return (
            f"INSERT INTO {target_table} ({col_list}) "
            f"SELECT {select_list} FROM {stage_table} WHERE TRUE "
            f"ON CONFLICT ({key_list}) DO UPDATE SET {set_clause}"
        )

    if norm in _MYSQL_FAMILY_DIALECTS:
        if on_conflict == "nothing" or not update_columns:
            return f"INSERT IGNORE INTO {target_table} ({col_list}) SELECT {select_list} FROM {stage_table}"
        set_clause = ", ".join(f"{col} = _sqlspec_stage.{col}" for col in update_columns)
        return (
            f"INSERT INTO {target_table} ({col_list}) "
            f"SELECT {select_list} FROM {stage_table} AS _sqlspec_stage "
            f"ON DUPLICATE KEY UPDATE {set_clause}"
        )

    on_clause = " AND ".join(f"_target.{key} = _stage.{key}" for key in conflict_keys)
    matched_clause = ""
    if on_conflict == "update" and update_columns:
        set_pairs = ", ".join(
            f"_target.{col} = CAST(_stage.{col} AS {type_casts[col]})"
            if (type_casts and col in type_casts)
            else f"_target.{col} = _stage.{col}"
            for col in update_columns
        )
        matched_clause = f" WHEN MATCHED THEN UPDATE SET {set_pairs}"
    stage_vals = ", ".join(
        f"CAST(_stage.{col} AS {type_casts[col]})" if (type_casts and col in type_casts) else f"_stage.{col}"
        for col in columns
    )
    not_matched_clause = f" WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({stage_vals})"
    terminator = ";" if norm in _MSSQL_FAMILY_DIALECTS else ""
    return f"MERGE INTO {target_table} _target USING {stage_table} _stage ON ({on_clause}){matched_clause}{not_matched_clause}{terminator}"


def build_drop_stage_table_sql(dialect: str, stage_table: str) -> str:
    """Build SQL statement to drop a staging table."""
    norm = (dialect or "").lower().strip()
    if norm in (_ORACLE_FAMILY_DIALECTS | {"db2"}):
        return f"DROP TABLE {stage_table}"
    return f"DROP TABLE IF EXISTS {stage_table}"


def filter_droppable_table_indexes(
    indexes: "Sequence[Mapping[str, Any]]", constraints: "Sequence[Mapping[str, Any]] | None" = None
) -> "list[dict[str, Any]]":
    """Filter data-dictionary index metadata to secondary indexes safe to drop and rebuild."""
    constraint_names: set[str] = set()
    if constraints:
        for constraint in constraints:
            cname = str(constraint.get("constraint_name") or constraint.get("name") or "").strip()
            if cname:
                constraint_names.add(cname.lower())

    droppable: list[dict[str, Any]] = []
    for idx in indexes:
        idx_name = str(idx.get("index_name") or idx.get("name") or "").strip()
        if not idx_name:
            continue
        if idx_name.upper() == "PRIMARY" or idx_name.lower().startswith("sqlite_autoindex_"):
            continue
        if idx.get("is_primary") or idx.get("primary"):
            continue
        if idx.get("is_exclusion") or idx.get("is_extension_owned") or idx.get("is_constraint_backed"):
            continue
        if idx.get("constraint_type") in {"PRIMARY KEY", "UNIQUE", "FOREIGN KEY", "EXCLUDE"}:
            continue
        if idx_name.lower() in constraint_names:
            continue
        droppable.append(dict(idx))
    return droppable


def build_drop_index_sql(
    dialect: str, index_meta: "Mapping[str, Any]", table: str, schema: str | None = None
) -> "str | None":
    """Build a dialect-appropriate DROP INDEX statement for a secondary index."""
    norm = (dialect or "").lower().strip()
    if norm == "bigquery":
        return None
    idx_name = str(index_meta.get("index_name") or index_meta.get("name") or "").strip()
    if not idx_name:
        return None
    resolved_schema = schema or (str(index_meta.get("schema_name") or "").strip() or None)
    if norm in _MSSQL_FAMILY_DIALECTS:
        qualified_table = f"[{resolved_schema}].[{table}]" if resolved_schema and "." not in table else f"[{table}]"
        return f"DROP INDEX [{idx_name}] ON {qualified_table}"
    if norm in _MYSQL_FAMILY_DIALECTS:
        qualified_table = f"`{resolved_schema}`.`{table}`" if resolved_schema and "." not in table else f"`{table}`"
        return f"DROP INDEX `{idx_name}` ON {qualified_table}"
    if norm in (_ORACLE_FAMILY_DIALECTS | {"db2"}):
        qualified_idx = f'"{resolved_schema}"."{idx_name}"' if resolved_schema else f'"{idx_name}"'
        return f"DROP INDEX {qualified_idx}"
    if norm in {"spanner", "cloudspanner"}:
        return f"DROP INDEX {idx_name}"
    qualified_idx = f'"{resolved_schema}"."{idx_name}"' if resolved_schema else f'"{idx_name}"'
    return f"DROP INDEX IF EXISTS {qualified_idx}"


def build_create_index_sql(
    dialect: str,
    index_meta: "Mapping[str, Any]",
    table: str,
    *,
    schema: str | None = None,
    concurrent: bool = False,
    in_transaction: bool = False,
    ddl_override: str | None = None,
) -> "str | None":
    """Build or adapt a dialect-specific CREATE INDEX statement to restore a dropped index."""
    norm = (dialect or "").lower().strip()
    if norm == "bigquery":
        return None
    raw_ddl = (
        ddl_override
        or index_meta.get("definition")
        or index_meta.get("native_sql")
        or index_meta.get("ddl")
        or index_meta.get("sql")
    )
    if isinstance(raw_ddl, str) and raw_ddl.strip():
        sql_text = raw_ddl.strip()
    else:
        idx_name = str(index_meta.get("index_name") or index_meta.get("name") or "").strip()
        cols = index_meta.get("columns") or index_meta.get("column_names") or []
        if isinstance(cols, str):
            col_list = cols
        elif isinstance(cols, Sequence):
            col_list = ", ".join(str(c) for c in cols if c)
        else:
            col_list = ""
        if not idx_name or not col_list:
            return None
        resolved_schema = schema or (str(index_meta.get("schema_name") or "").strip() or None)
        is_unique = bool(index_meta.get("is_unique") or index_meta.get("unique"))
        unique_kw = "UNIQUE " if is_unique else ""
        if norm in _MSSQL_FAMILY_DIALECTS:
            qualified_table = f"[{resolved_schema}].[{table}]" if resolved_schema and "." not in table else f"[{table}]"
            sql_text = f"CREATE {unique_kw}INDEX [{idx_name}] ON {qualified_table} ({col_list})"
        elif norm in _MYSQL_FAMILY_DIALECTS:
            qualified_table = f"`{resolved_schema}`.`{table}`" if resolved_schema and "." not in table else f"`{table}`"
            sql_text = f"CREATE {unique_kw}INDEX `{idx_name}` ON {qualified_table} ({col_list})"
        else:
            qualified_table = f'"{resolved_schema}"."{table}"' if resolved_schema and "." not in table else table
            sql_text = f'CREATE {unique_kw}INDEX "{idx_name}" ON {qualified_table} ({col_list})'

    if concurrent and not in_transaction and norm in {"postgres", "postgresql", "psycopg", "asyncpg", "psqlpy"}:
        if _CREATE_UNIQUE_INDEX_RE.match(sql_text):
            sql_text = _CREATE_UNIQUE_INDEX_RE.sub("CREATE UNIQUE INDEX CONCURRENTLY ", sql_text, count=1)
        elif _CREATE_INDEX_RE.match(sql_text):
            sql_text = _CREATE_INDEX_RE.sub("CREATE INDEX CONCURRENTLY ", sql_text, count=1)
    return sql_text


def _extract_metadata_rows(result: Any) -> "list[dict[str, Any]]":
    if result is None:
        return []
    data = getattr(result, "data", result)
    if isinstance(data, list):
        return [dict(item) if isinstance(item, Mapping) else {} for item in data]
    if isinstance(data, Sequence):
        return [dict(item) if isinstance(item, Mapping) else {} for item in data]
    return []


def _extract_ddl_text(result: Any) -> "str | None":
    if isinstance(result, str) and result.strip():
        return result.strip()
    rows = _extract_metadata_rows(result)
    for row in rows:
        for key in ("ddl", "definition", "native_sql", "sql"):
            val = row.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return None


def _safe_driver_in_transaction(driver: Any) -> bool:
    checker = getattr(driver, "is_in_transaction", None)
    if callable(checker):
        with contextlib.suppress(Exception):
            return bool(checker())
    conn_checker = getattr(driver, "_connection_in_transaction", None)
    if callable(conn_checker):
        with contextlib.suppress(Exception):
            return bool(conn_checker())
    return False


def _split_schema_and_table(table: str, explicit_schema: str | None) -> "tuple[str | None, str]":
    clean = table.strip()
    if explicit_schema is not None:
        return explicit_schema, clean.rsplit(".", 1)[-1].strip('"`[]')
    if "." in clean:
        schema_part, table_part = clean.split(".", 1)
        return schema_part.strip('"`[]'), table_part.strip('"`[]')
    return None, clean.strip('"`[]')


class SyncWithoutIndexesContext:
    """Synchronous context manager that drops secondary indexes before a bulk load and rebuilds them on exit."""

    __slots__ = ("_concurrent", "_driver", "_rebuild_statements", "_schema", "_table")

    def __init__(self, driver: Any, table: str, *, schema: str | None = None, concurrent: bool = False) -> None:
        self._driver = driver
        self._table = table
        self._schema = schema
        self._concurrent = concurrent
        self._rebuild_statements: list[str] = []

    def __enter__(self) -> Self:
        dialect = str(getattr(self._driver, "dialect", "") or "").lower().strip()
        if dialect == "bigquery":
            return self
        schema_name, table_name = _split_schema_and_table(self._table, self._schema)
        dd = getattr(self._driver, "data_dictionary", None)
        if dd is None:
            return self
        indexes_rows = _extract_metadata_rows(dd.get_indexes(self._driver, table=table_name, schema=schema_name))
        constraints_rows: list[dict[str, Any]] = []
        with contextlib.suppress(Exception):
            constraints_rows = _extract_metadata_rows(
                dd.get_constraints(self._driver, table=table_name, schema=schema_name)
            )
        droppable = filter_droppable_table_indexes(indexes_rows, constraints_rows)
        in_tx = _safe_driver_in_transaction(self._driver)
        for idx in droppable:
            ddl_override: str | None = None
            idx_name = str(idx.get("index_name") or idx.get("name") or "").strip()
            if (
                idx_name
                and not (idx.get("definition") or idx.get("native_sql") or idx.get("ddl"))
                and hasattr(dd, "get_ddl")
            ):
                with contextlib.suppress(Exception):
                    ddl_override = _extract_ddl_text(
                        dd.get_ddl(self._driver, idx_name, schema=schema_name, object_type="INDEX")
                    )
            create_sql = build_create_index_sql(
                dialect,
                idx,
                table_name,
                schema=schema_name,
                concurrent=self._concurrent,
                in_transaction=in_tx,
                ddl_override=ddl_override,
            )
            drop_sql = build_drop_index_sql(dialect, idx, table_name, schema=schema_name)
            if drop_sql and create_sql:
                self._driver.execute(drop_sql)
                self._rebuild_statements.append(create_sql)
        return self

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> Literal[False]:
        in_tx = _safe_driver_in_transaction(self._driver)
        rebuild_errors: list[Exception] = []
        for stmt in self._rebuild_statements:
            effective_stmt = stmt
            if in_tx and " INDEX CONCURRENTLY " in effective_stmt.upper():
                effective_stmt = re.sub(
                    r"\bINDEX\s+CONCURRENTLY\s+", "INDEX ", effective_stmt, count=1, flags=re.IGNORECASE
                )
            try:
                self._driver.execute(effective_stmt)
            except Exception as err:
                rebuild_errors.append(err)
        if exc_type is None and rebuild_errors:
            raise rebuild_errors[0]
        return False


class AsyncWithoutIndexesContext:
    """Asynchronous context manager that drops secondary indexes before a bulk load and rebuilds them on exit."""

    __slots__ = ("_concurrent", "_driver", "_rebuild_statements", "_schema", "_table")

    def __init__(self, driver: Any, table: str, *, schema: str | None = None, concurrent: bool = False) -> None:
        self._driver = driver
        self._table = table
        self._schema = schema
        self._concurrent = concurrent
        self._rebuild_statements: list[str] = []

    async def __aenter__(self) -> Self:
        dialect = str(getattr(self._driver, "dialect", "") or "").lower().strip()
        if dialect == "bigquery":
            return self
        schema_name, table_name = _split_schema_and_table(self._table, self._schema)
        dd = getattr(self._driver, "data_dictionary", None)
        if dd is None:
            return self
        indexes_rows = _extract_metadata_rows(await dd.get_indexes(self._driver, table=table_name, schema=schema_name))
        constraints_rows: list[dict[str, Any]] = []
        with contextlib.suppress(Exception):
            constraints_rows = _extract_metadata_rows(
                await dd.get_constraints(self._driver, table=table_name, schema=schema_name)
            )
        droppable = filter_droppable_table_indexes(indexes_rows, constraints_rows)
        in_tx = _safe_driver_in_transaction(self._driver)
        for idx in droppable:
            ddl_override: str | None = None
            idx_name = str(idx.get("index_name") or idx.get("name") or "").strip()
            if (
                idx_name
                and not (idx.get("definition") or idx.get("native_sql") or idx.get("ddl"))
                and hasattr(dd, "get_ddl")
            ):
                with contextlib.suppress(Exception):
                    ddl_override = _extract_ddl_text(
                        await dd.get_ddl(self._driver, idx_name, schema=schema_name, object_type="INDEX")
                    )
            create_sql = build_create_index_sql(
                dialect,
                idx,
                table_name,
                schema=schema_name,
                concurrent=self._concurrent,
                in_transaction=in_tx,
                ddl_override=ddl_override,
            )
            drop_sql = build_drop_index_sql(dialect, idx, table_name, schema=schema_name)
            if drop_sql and create_sql:
                await self._driver.execute(drop_sql)
                self._rebuild_statements.append(create_sql)
        return self

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> Literal[False]:
        in_tx = _safe_driver_in_transaction(self._driver)
        rebuild_errors: list[Exception] = []
        for stmt in self._rebuild_statements:
            effective_stmt = stmt
            if in_tx and " INDEX CONCURRENTLY " in effective_stmt.upper():
                effective_stmt = re.sub(
                    r"\bINDEX\s+CONCURRENTLY\s+", "INDEX ", effective_stmt, count=1, flags=re.IGNORECASE
                )
            try:
                await self._driver.execute(effective_stmt)
            except Exception as err:
                rebuild_errors.append(err)
        if exc_type is None and rebuild_errors:
            raise rebuild_errors[0]
        return False


def execute_staged_load_sync(
    driver: Any,
    table: str,
    source: Any,
    *,
    raw_loader: "Callable[..., StorageBridgeJob]",
    partitioner: "dict[str, object] | None" = None,
    overwrite: bool = False,
    telemetry: "StorageTelemetry | None" = None,
    stage_table: "bool | str | None" = None,
    conflict_keys: "Sequence[str] | None" = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: "Sequence[str] | None" = None,
    drop_stage_table: bool = True,
    drop_indexes: bool = False,
    concurrent_indexes: bool = False,
    type_casts: "Mapping[str, str] | None" = None,
    multi_worker: bool = False,
) -> "StorageBridgeJob":
    """Orchestrate index drop/rebuild and staging table creation/merge/cleanup for sync bulk loads."""
    if drop_indexes:
        with SyncWithoutIndexesContext(driver, table, concurrent=concurrent_indexes):
            return execute_staged_load_sync(
                driver,
                table,
                source,
                raw_loader=raw_loader,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
                multi_worker=multi_worker,
            )

    dialect = str(getattr(driver, "dialect", "") or "").lower().strip()
    resolved_stage = resolve_stage_table_name(table, stage_table, conflict_keys=conflict_keys, dialect=dialect)
    if resolved_stage is None:
        return raw_loader(table, source, partitioner=partitioner, overwrite=overwrite, telemetry=telemetry)

    arrow_table = coerce_arrow_table(source)
    columns = list(arrow_table.column_names)
    for ddl in build_stage_table_ddl(dialect, table, resolved_stage, multi_worker=multi_worker):
        driver.execute(ddl)
    try:
        if overwrite and not conflict_keys:
            driver.execute(f"DELETE FROM {table}")
        job = raw_loader(resolved_stage, arrow_table, partitioner=partitioner, overwrite=False, telemetry=telemetry)
        if columns and arrow_table.num_rows > 0:
            merge_sql = build_stage_merge_sql(
                dialect,
                table,
                resolved_stage,
                columns,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                type_casts=type_casts,
            )
            driver.execute(merge_sql)
        job.telemetry["destination"] = table
        return job
    finally:
        if drop_stage_table:
            with contextlib.suppress(Exception):
                driver.execute(build_drop_stage_table_sql(dialect, resolved_stage))


async def execute_staged_load_async(
    driver: Any,
    table: str,
    source: Any,
    *,
    raw_loader: "Callable[..., Any]",
    partitioner: "dict[str, object] | None" = None,
    overwrite: bool = False,
    telemetry: "StorageTelemetry | None" = None,
    stage_table: "bool | str | None" = None,
    conflict_keys: "Sequence[str] | None" = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: "Sequence[str] | None" = None,
    drop_stage_table: bool = True,
    drop_indexes: bool = False,
    concurrent_indexes: bool = False,
    type_casts: "Mapping[str, str] | None" = None,
    multi_worker: bool = False,
) -> "StorageBridgeJob":
    """Orchestrate index drop/rebuild and staging table creation/merge/cleanup for async bulk loads."""
    if drop_indexes:
        async with AsyncWithoutIndexesContext(driver, table, concurrent=concurrent_indexes):
            return await execute_staged_load_async(
                driver,
                table,
                source,
                raw_loader=raw_loader,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
                multi_worker=multi_worker,
            )

    dialect = str(getattr(driver, "dialect", "") or "").lower().strip()
    resolved_stage = resolve_stage_table_name(table, stage_table, conflict_keys=conflict_keys, dialect=dialect)
    if resolved_stage is None:
        return cast(
            "StorageBridgeJob",
            await raw_loader(table, source, partitioner=partitioner, overwrite=overwrite, telemetry=telemetry),
        )

    arrow_table = coerce_arrow_table(source)
    columns = list(arrow_table.column_names)
    for ddl in build_stage_table_ddl(dialect, table, resolved_stage, multi_worker=multi_worker):
        await driver.execute(ddl)
    try:
        if overwrite and not conflict_keys:
            await driver.execute(f"DELETE FROM {table}")
        job = cast(
            "StorageBridgeJob",
            await raw_loader(
                resolved_stage, arrow_table, partitioner=partitioner, overwrite=False, telemetry=telemetry
            ),
        )
        if columns and arrow_table.num_rows > 0:
            merge_sql = build_stage_merge_sql(
                dialect,
                table,
                resolved_stage,
                columns,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                type_casts=type_casts,
            )
            await driver.execute(merge_sql)
        job.telemetry["destination"] = table
        return job
    finally:
        if drop_stage_table:
            with contextlib.suppress(Exception):
                await driver.execute(build_drop_stage_table_sql(dialect, resolved_stage))


def _record_checkpoint_sync(
    driver: Any,
    dialect: str,
    checkpoint_table: str,
    phase: str,
    shard_uri: str,
    rows_loaded: int,
    bytes_processed: int,
    duration_s: float,
) -> None:
    sql_text = build_checkpoint_insert_sql(dialect, checkpoint_table)
    driver.execute(
        sql_text,
        {
            "phase": phase,
            "shard_uri": shard_uri,
            "rows_loaded": int(rows_loaded),
            "bytes_processed": int(bytes_processed),
            "duration_s": float(duration_s),
        },
    )


async def _record_checkpoint_async(
    driver: Any,
    dialect: str,
    checkpoint_table: str,
    phase: str,
    shard_uri: str,
    rows_loaded: int,
    bytes_processed: int,
    duration_s: float,
) -> None:
    sql_text = build_checkpoint_insert_sql(dialect, checkpoint_table)
    await driver.execute(
        sql_text,
        {
            "phase": phase,
            "shard_uri": shard_uri,
            "rows_loaded": int(rows_loaded),
            "bytes_processed": int(bytes_processed),
            "duration_s": float(duration_s),
        },
    )


def _read_shard_bytes_and_decode(
    shard_uri: str, file_format: "StorageFormat", storage_options: "dict[str, Any] | None", arrow_schema: Any
) -> "tuple[str, Any, int, float]":
    t0 = perf_counter()
    pipeline = SyncStoragePipeline()
    backend, path, _backend_name = pipeline._backend(shard_uri, storage_options)
    payload = backend.read_bytes_sync(path)
    table = decode_arrow_payload(payload, file_format, arrow_schema=arrow_schema)
    return shard_uri, table, len(payload), perf_counter() - t0


def _decode_shard_in_process_worker(
    args: "tuple[str, StorageFormat, dict[str, Any] | None]",
) -> "tuple[str, bytes, int, float]":
    shard_uri, file_format, storage_options = args
    t0 = perf_counter()
    pipeline = SyncStoragePipeline()
    backend, path, _backend_name = pipeline._backend(shard_uri, storage_options)
    payload = backend.read_bytes_sync(path)
    table = decode_arrow_payload(payload, file_format)
    ipc_bytes = encode_arrow_payload(table, "arrow-ipc", compression=None)
    return shard_uri, ipc_bytes, len(payload), perf_counter() - t0


def _is_multi_shard_source(source: "StorageDestination | Sequence[StorageDestination]") -> tuple[bool, str, bool]:
    """Return ``(is_source_sequence, source_str, has_glob_or_prefix)`` for a storage destination."""
    if isinstance(source, Sequence) and not isinstance(source, (str, bytes, bytearray)):
        return True, "", True
    source_str = source.as_posix() if isinstance(source, Path) else str(source)
    if any(ch in source_str for ch in ("*", "?", "[")) or source_str.endswith("/"):
        return False, source_str, True
    if "://" not in source_str:
        with contextlib.suppress(Exception):
            if Path(source_str).expanduser().is_dir():
                return False, source_str, True
    return False, source_str, False


def execute_load_from_storage_sync(
    driver: Any,
    table: str,
    source: "StorageDestination | Sequence[StorageDestination]",
    *,
    file_format: "StorageFormat",
    partitioner: "dict[str, object] | None" = None,
    overwrite: bool = False,
    storage_options: "dict[str, Any] | None" = None,
    stage_table: "bool | str | None" = None,
    conflict_keys: "Sequence[str] | None" = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: "Sequence[str] | None" = None,
    drop_stage_table: bool = True,
    drop_indexes: bool = False,
    concurrent_indexes: bool = False,
    checkpoint_table: str | None = None,
    checkpoint_phase: str = "default",
    max_workers: int = 1,
    executor: Literal["thread", "process"] = "thread",
    arrow_schema: Any = None,
    type_casts: "Mapping[str, str] | None" = None,
) -> "StorageBridgeJob":
    """Execute single-shard or multi-shard synchronous storage ingestion with checkpointing, staging, and index management."""
    is_source_sequence = isinstance(source, Sequence) and not isinstance(source, (str, bytes, bytearray))
    source_str = "" if is_source_sequence else (source.as_posix() if isinstance(source, Path) else str(source))
    has_glob_or_prefix = (
        is_source_sequence or any(ch in source_str for ch in ("*", "?", "[")) or source_str.endswith("/")
    )
    if not has_glob_or_prefix and "://" not in source_str:
        with contextlib.suppress(Exception):
            if Path(source_str).expanduser().is_dir():
                has_glob_or_prefix = True

    is_simple_single_load = (
        not has_glob_or_prefix
        and checkpoint_table is None
        and max_workers <= 1
        and stage_table is None
        and conflict_keys is None
        and not drop_indexes
        and arrow_schema is None
    )
    if is_simple_single_load:
        arrow_table, inbound = (
            driver._read_storage_arrow(source, file_format=file_format)
            if storage_options is None
            else driver._read_storage_arrow(source, file_format=file_format, storage_options=storage_options)
        )
        return cast(
            "StorageBridgeJob",
            driver.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound),
        )

    if drop_indexes:
        with SyncWithoutIndexesContext(driver, table, concurrent=concurrent_indexes):
            return execute_load_from_storage_sync(
                driver,
                table,
                source,
                file_format=file_format,
                partitioner=partitioner,
                overwrite=overwrite,
                storage_options=storage_options,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                checkpoint_table=checkpoint_table,
                checkpoint_phase=checkpoint_phase,
                max_workers=max_workers,
                executor=executor,
                arrow_schema=arrow_schema,
                type_casts=type_casts,
            )

    pipeline = driver._storage_pipeline()
    if is_source_sequence:
        shards = [
            item.as_posix() if isinstance(item, Path) else str(item)
            for item in cast("Sequence[StorageDestination]", source)
        ]
    elif has_glob_or_prefix:
        shards = pipeline.list_sources(source, file_format=file_format, storage_options=storage_options)
    else:
        shards = [source_str]
    dialect = str(getattr(driver, "dialect", "") or "").lower().strip()
    completed_uris: set[str] = set()
    if checkpoint_table:
        driver.execute(build_checkpoint_table_ddl(dialect, checkpoint_table))
        res = driver.execute(build_checkpoint_select_sql(checkpoint_table), {"phase": checkpoint_phase})
        for row in res.data or []:
            uri_val = row.get("shard_uri") if isinstance(row, Mapping) else row[0]
            if uri_val is not None:
                completed_uris.add(str(uri_val))

    pending_shards = [s for s in shards if s not in completed_uris]
    shards_skipped = len(shards) - len(pending_shards)

    resolved_stage = resolve_stage_table_name(table, stage_table, conflict_keys=conflict_keys, dialect=dialect)
    effective_target = resolved_stage or table
    if resolved_stage is not None:
        for ddl in build_stage_table_ddl(dialect, table, resolved_stage, multi_worker=max_workers > 1):
            driver.execute(ddl)

    total_rows = 0
    total_bytes = 0
    read_duration = 0.0
    write_duration = 0.0
    observed_columns: list[str] = []
    start_wall = perf_counter()

    try:
        if overwrite and pending_shards and not conflict_keys:
            driver.execute(f"DELETE FROM {table}")

        decoded_shards: list[tuple[str, Any, int, float]] = []
        if max_workers > 1 and len(pending_shards) > 1:
            if executor == "process":
                mp_ctx = multiprocessing.get_context("spawn")
                with ProcessPoolExecutor(
                    max_workers=max_workers,
                    mp_context=mp_ctx,
                    initializer=init_spawn_worker_thread_budget,
                    initargs=(max_workers,),
                ) as pool:
                    worker_args = [(s, file_format, storage_options) for s in pending_shards]
                    for shard_uri, ipc_bytes, raw_bytes, r_dur in pool.map(
                        _decode_shard_in_process_worker, worker_args
                    ):
                        tbl = decode_arrow_payload(ipc_bytes, "arrow-ipc", arrow_schema=arrow_schema)
                        decoded_shards.append((shard_uri, tbl, raw_bytes, r_dur))
            else:
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    futures = [
                        pool.submit(_read_shard_bytes_and_decode, s, file_format, storage_options, arrow_schema)
                        for s in pending_shards
                    ]
                    decoded_shards = [f.result() for f in futures]
        else:
            for shard_uri in pending_shards:
                decoded_shards.append(
                    _read_shard_bytes_and_decode(shard_uri, file_format, storage_options, arrow_schema)
                )

        for idx, (shard_uri, shard_table, shard_bytes, r_dur) in enumerate(decoded_shards):
            read_duration += r_dur
            if not observed_columns and hasattr(shard_table, "column_names"):
                observed_columns = list(shard_table.column_names)
            w0 = perf_counter()
            shard_overwrite = bool(overwrite and idx == 0 and resolved_stage is None and not conflict_keys)
            shard_job = driver.load_from_arrow(
                effective_target, shard_table, partitioner=partitioner, overwrite=shard_overwrite
            )
            w_dur = perf_counter() - w0
            write_duration += w_dur
            s_rows = int(shard_job.telemetry.get("rows_processed", shard_table.num_rows))
            total_rows += s_rows
            total_bytes += int(shard_bytes)
            if checkpoint_table:
                _record_checkpoint_sync(
                    driver, dialect, checkpoint_table, checkpoint_phase, shard_uri, s_rows, shard_bytes, r_dur + w_dur
                )

        if resolved_stage is not None and observed_columns and total_rows > 0:
            merge_sql = build_stage_merge_sql(
                dialect,
                table,
                resolved_stage,
                observed_columns,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                type_casts=type_casts,
            )
            w0 = perf_counter()
            driver.execute(merge_sql)
            write_duration += perf_counter() - w0
    finally:
        if resolved_stage is not None and drop_stage_table:
            with contextlib.suppress(Exception):
                driver.execute(build_drop_stage_table_sql(dialect, resolved_stage))

    telemetry_payload = ingest_telemetry_from_batches(
        total_rows,
        total_bytes,
        duration_s=perf_counter() - start_wall,
        format_label=file_format,
        read_duration_s=read_duration,
        write_duration_s=write_duration,
        shards_processed=len(pending_shards),
        shards_skipped=shards_skipped,
    )
    telemetry_payload["destination"] = table
    attach_partition_telemetry(telemetry_payload, partitioner)
    return create_storage_job(telemetry_payload)


async def execute_load_from_storage_async(
    driver: Any,
    table: str,
    source: "StorageDestination | Sequence[StorageDestination]",
    *,
    file_format: "StorageFormat",
    partitioner: "dict[str, object] | None" = None,
    overwrite: bool = False,
    storage_options: "dict[str, Any] | None" = None,
    stage_table: "bool | str | None" = None,
    conflict_keys: "Sequence[str] | None" = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: "Sequence[str] | None" = None,
    drop_stage_table: bool = True,
    drop_indexes: bool = False,
    concurrent_indexes: bool = False,
    checkpoint_table: str | None = None,
    checkpoint_phase: str = "default",
    max_workers: int = 1,
    executor: Literal["thread", "process"] = "thread",
    arrow_schema: Any = None,
    type_casts: "Mapping[str, str] | None" = None,
) -> "StorageBridgeJob":
    """Execute single-shard or multi-shard asynchronous storage ingestion with checkpointing, staging, and index management."""
    is_source_sequence, source_str, has_glob_or_prefix = _is_multi_shard_source(source)

    is_simple_single_load = (
        not has_glob_or_prefix
        and checkpoint_table is None
        and max_workers <= 1
        and stage_table is None
        and conflict_keys is None
        and not drop_indexes
        and arrow_schema is None
    )
    if is_simple_single_load:
        arrow_table, inbound = (
            await driver._read_storage_arrow(source, file_format=file_format)
            if storage_options is None
            else await driver._read_storage_arrow(source, file_format=file_format, storage_options=storage_options)
        )
        return cast(
            "StorageBridgeJob",
            await driver.load_from_arrow(
                table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
            ),
        )

    if drop_indexes:
        async with AsyncWithoutIndexesContext(driver, table, concurrent=concurrent_indexes):
            return await execute_load_from_storage_async(
                driver,
                table,
                source,
                file_format=file_format,
                partitioner=partitioner,
                overwrite=overwrite,
                storage_options=storage_options,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                checkpoint_table=checkpoint_table,
                checkpoint_phase=checkpoint_phase,
                max_workers=max_workers,
                executor=executor,
                arrow_schema=arrow_schema,
                type_casts=type_casts,
            )

    pipeline = driver._storage_pipeline()
    if is_source_sequence:
        shards = [
            item.as_posix() if isinstance(item, Path) else str(item)
            for item in cast("Sequence[StorageDestination]", source)
        ]
    elif has_glob_or_prefix:
        shards = await pipeline.list_sources_async(source, file_format=file_format, storage_options=storage_options)
    else:
        shards = [source_str]
    dialect = str(getattr(driver, "dialect", "") or "").lower().strip()
    completed_uris: set[str] = set()
    if checkpoint_table:
        await driver.execute(build_checkpoint_table_ddl(dialect, checkpoint_table))
        res = await driver.execute(build_checkpoint_select_sql(checkpoint_table), {"phase": checkpoint_phase})
        for row in res.data or []:
            uri_val = row.get("shard_uri") if isinstance(row, Mapping) else row[0]
            if uri_val is not None:
                completed_uris.add(str(uri_val))

    pending_shards = [s for s in shards if s not in completed_uris]
    shards_skipped = len(shards) - len(pending_shards)

    resolved_stage = resolve_stage_table_name(table, stage_table, conflict_keys=conflict_keys, dialect=dialect)
    effective_target = resolved_stage or table
    if resolved_stage is not None:
        for ddl in build_stage_table_ddl(dialect, table, resolved_stage, multi_worker=max_workers > 1):
            await driver.execute(ddl)

    total_rows = 0
    total_bytes = 0
    read_duration = 0.0
    write_duration = 0.0
    observed_columns: list[str] = []
    start_wall = perf_counter()

    try:
        if overwrite and pending_shards and not conflict_keys:
            await driver.execute(f"DELETE FROM {table}")

        loop = asyncio.get_running_loop()
        decoded_shards: list[tuple[str, Any, int, float]] = []
        if max_workers > 1 and len(pending_shards) > 1:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                tasks = [
                    loop.run_in_executor(
                        pool, _read_shard_bytes_and_decode, s, file_format, storage_options, arrow_schema
                    )
                    for s in pending_shards
                ]
                decoded_shards = list(await asyncio.gather(*tasks))
        else:
            for shard_uri in pending_shards:
                decoded_shards.append(
                    await loop.run_in_executor(
                        None, _read_shard_bytes_and_decode, shard_uri, file_format, storage_options, arrow_schema
                    )
                )

        for idx, (shard_uri, shard_table, shard_bytes, r_dur) in enumerate(decoded_shards):
            read_duration += r_dur
            if not observed_columns and hasattr(shard_table, "column_names"):
                observed_columns = list(shard_table.column_names)
            w0 = perf_counter()
            shard_overwrite = bool(overwrite and idx == 0 and resolved_stage is None and not conflict_keys)
            shard_job = await driver.load_from_arrow(
                effective_target, shard_table, partitioner=partitioner, overwrite=shard_overwrite
            )
            w_dur = perf_counter() - w0
            write_duration += w_dur
            s_rows = int(shard_job.telemetry.get("rows_processed", shard_table.num_rows))
            total_rows += s_rows
            total_bytes += int(shard_bytes)
            if checkpoint_table:
                await _record_checkpoint_async(
                    driver, dialect, checkpoint_table, checkpoint_phase, shard_uri, s_rows, shard_bytes, r_dur + w_dur
                )

        if resolved_stage is not None and observed_columns and total_rows > 0:
            merge_sql = build_stage_merge_sql(
                dialect,
                table,
                resolved_stage,
                observed_columns,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                type_casts=type_casts,
            )
            w0 = perf_counter()
            await driver.execute(merge_sql)
            write_duration += perf_counter() - w0
    finally:
        if resolved_stage is not None and drop_stage_table:
            with contextlib.suppress(Exception):
                await driver.execute(build_drop_stage_table_sql(dialect, resolved_stage))

    telemetry_payload = ingest_telemetry_from_batches(
        total_rows,
        total_bytes,
        duration_s=perf_counter() - start_wall,
        format_label=file_format,
        read_duration_s=read_duration,
        write_duration_s=write_duration,
        shards_processed=len(pending_shards),
        shards_skipped=shards_skipped,
    )
    telemetry_payload["destination"] = table
    attach_partition_telemetry(telemetry_payload, partitioner)
    return create_storage_job(telemetry_payload)
