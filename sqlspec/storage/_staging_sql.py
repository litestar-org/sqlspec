"""Compiled staging-table, checkpoint-ledger, and worker thread-budget SQL builders."""

import os
import re
from collections.abc import Mapping, Sequence
from typing import Final, Literal

from sqlspec.data_dictionary._registry import normalize_adapter_dialect_name
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.text import split_qualified_identifier
from sqlspec.utils.uuids import uuid4

__all__ = (
    "SAP_DIALECTS",
    "build_checkpoint_insert_sql",
    "build_checkpoint_select_sql",
    "build_checkpoint_table_ddl",
    "build_drop_stage_table_sql",
    "build_stage_merge_sql",
    "build_stage_table_ddl",
    "build_teradata_checkpoint_probe",
    "compute_worker_thread_budget",
    "normalize_storage_dialect",
    "resolve_stage_table_name",
)

_PG_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({"postgres", "cockroachdb", "spangres"})
_ON_CONFLICT_DIALECTS: Final[frozenset[str]] = _PG_FAMILY_DIALECTS | frozenset({"duckdb", "sqlite"})
_MYSQL_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({"mysql", "mariadb"})
SAP_DIALECTS: Final[frozenset[str]] = frozenset({"sybase", "sqlanywhere"})
_QUALIFIED_TABLE_PARTS: Final[int] = 2
_TERADATA_IDENTIFIER_PART_RE: Final = re.compile(r'"(?:[^"]|"")*"|\[(?:[^\]]|\]\])*\]|[^.\s]+')
_TERADATA_TABLE_EXISTS_SQL: Final[str] = (
    "LOCKING ROW FOR ACCESS SELECT 1 FROM DBC.TablesV "
    "WHERE UPPER(TRIM(DatabaseName)) = UPPER(TRIM(COALESCE(CAST(? AS VARCHAR(128)), DATABASE))) "
    "AND UPPER(TRIM(TableName)) = UPPER(TRIM(?)) "
    "AND TableKind IN ('T', 'O')"
)


def normalize_storage_dialect(dialect: str | None) -> str:
    """Normalize a dialect or adapter identifier to its canonical storage-SQL dialect key.

    Args:
        dialect: Dialect or adapter name string.

    Returns:
        Canonical lowercase dialect key.
    """
    return normalize_adapter_dialect_name(dialect)


def compute_worker_thread_budget(
    max_workers: int, cpu_count: int | None = None, *, reserve_cpus: int = 0
) -> dict[str, str]:
    """Compute per-worker native thread pool environment variables to prevent oversubscription.

    Args:
        max_workers: Number of concurrent ingestion workers.
        cpu_count: Optional logical CPU count override.
        reserve_cpus: Number of CPU cores reserved for the host OS and main event loop.

    Returns:
        Dictionary of thread-budget environment variables and string values.
    """
    total_cpus = cpu_count if cpu_count is not None and cpu_count > 0 else (os.cpu_count() or 1)
    resolved_cpus = max(1, total_cpus - max(0, reserve_cpus))
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


def build_teradata_checkpoint_probe(checkpoint_table: str) -> tuple[str, tuple[str | None, str]]:
    """Build the ``DBC.TablesV`` existence probe query and bind parameters for Teradata.

    Args:
        checkpoint_table: Qualified or unqualified Teradata checkpoint table identifier.

    Returns:
        Tuple of ``(probe_sql, (database_name_or_none, table_name))``.

    Raises:
        SQLSpecError: If ``checkpoint_table`` is empty or exceeds two identifier segments.
    """
    stripped = checkpoint_table.strip()
    parts = split_qualified_identifier(stripped, quote_chars='"', allow_bracket_quotes=True)
    if not parts or len(parts) > _QUALIFIED_TABLE_PARTS:
        msg = "Expected a table or database.table identifier"
        raise SQLSpecError(msg)
    raw_parts = _TERADATA_IDENTIFIER_PART_RE.findall(stripped)
    normalized = [
        part if raw.startswith(('"', "[")) else part.upper() for part, raw in zip(parts, raw_parts, strict=True)
    ]
    params = (normalized[0], normalized[1]) if len(normalized) == _QUALIFIED_TABLE_PARTS else (None, normalized[0])
    return _TERADATA_TABLE_EXISTS_SQL, params


def build_checkpoint_table_ddl(dialect: str, checkpoint_table: str) -> str:
    """Build DDL to create a multi-shard ingestion checkpoint table if it does not exist.

    Args:
        dialect: Target dialect or adapter name.
        checkpoint_table: Checkpoint ledger table name.

    Returns:
        Dialect-specific ``CREATE TABLE`` DDL statement.
    """
    norm = normalize_storage_dialect(dialect)
    if norm == "teradata":
        return (
            f"CREATE MULTISET TABLE {checkpoint_table} ("
            "phase VARCHAR(128) NOT NULL, shard_uri VARCHAR(1024) NOT NULL, "
            "rows_loaded BIGINT NOT NULL, bytes_processed BIGINT NOT NULL, duration_s DOUBLE PRECISION NOT NULL, "
            "completed_at TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6)) "
            "UNIQUE PRIMARY INDEX (phase, shard_uri)"
        )
    if norm == "sybase":
        create_sql = (
            f"CREATE TABLE {checkpoint_table} ("
            "phase VARCHAR(128) NOT NULL, "
            "shard_uri VARCHAR(1024) NOT NULL, "
            "rows_loaded BIGINT NOT NULL, "
            "bytes_processed BIGINT NOT NULL, "
            "duration_s FLOAT NOT NULL, "
            "completed_at BIGDATETIME DEFAULT GETDATE(), "
            "PRIMARY KEY (phase, shard_uri))"
        )
        object_name = checkpoint_table.replace("'", "''")
        escaped_create = create_sql.replace("'", "''")
        return f"IF OBJECT_ID('{object_name}') IS NULL EXECUTE ('{escaped_create}')"
    if norm == "mssql":
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
    norm = normalize_storage_dialect(dialect)
    if norm == "teradata":
        return (
            f"MERGE INTO {checkpoint_table} AS tgt USING (SELECT "
            "CAST(:phase AS VARCHAR(128)) AS phase, CAST(:shard_uri AS VARCHAR(1024)) AS shard_uri, "
            "CAST(:rows_loaded AS BIGINT) AS rows_loaded, CAST(:bytes_processed AS BIGINT) AS bytes_processed, "
            "CAST(:duration_s AS DOUBLE PRECISION) AS duration_s) AS src "
            "ON (tgt.phase = src.phase AND tgt.shard_uri = src.shard_uri) "
            "WHEN MATCHED THEN UPDATE SET rows_loaded = src.rows_loaded, bytes_processed = src.bytes_processed, "
            "duration_s = src.duration_s, completed_at = CURRENT_TIMESTAMP(6) "
            "WHEN NOT MATCHED THEN INSERT (phase, shard_uri, rows_loaded, bytes_processed, duration_s) "
            "VALUES (src.phase, src.shard_uri, src.rows_loaded, src.bytes_processed, src.duration_s)"
        )
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
    stage_table: bool | str | None,
    *,
    conflict_keys: Sequence[str] | None = None,
    dialect: str | None = None,
) -> str | None:
    """Resolve the staging table name when staged bulk loading or conflict resolution is requested."""
    if isinstance(stage_table, str):
        stripped = stage_table.strip()
        return stripped or None
    if stage_table is False:
        return None
    norm_dialect = normalize_storage_dialect(dialect)
    needs_stage = stage_table is True or (bool(conflict_keys) and norm_dialect != "spanner")
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
) -> list[str]:
    """Build dialect-specific DDL statements to create an empty staging table matching ``target_table``."""
    norm = normalize_storage_dialect(dialect)
    if norm == "teradata":
        return [f"CREATE MULTISET TABLE {stage_table} AS (SELECT * FROM {target_table}) WITH NO DATA"]
    if norm in _PG_FAMILY_DIALECTS:
        return [f"CREATE UNLOGGED TABLE IF NOT EXISTS {stage_table} (LIKE {target_table} INCLUDING DEFAULTS)"]
    if norm in {"duckdb", "sqlite"}:
        if multi_worker or "." in stage_table:
            return [f"CREATE TABLE IF NOT EXISTS {stage_table} AS SELECT * FROM {target_table} WHERE 1=0"]
        return [f"CREATE TEMP TABLE IF NOT EXISTS {stage_table} AS SELECT * FROM {target_table} WHERE 1=0"]
    if norm in _MYSQL_FAMILY_DIALECTS:
        if multi_worker:
            return [f"CREATE TABLE IF NOT EXISTS {stage_table} LIKE {target_table}"]
        return [f"CREATE TEMPORARY TABLE IF NOT EXISTS {stage_table} LIKE {target_table}"]
    if norm == "oracle":
        return [f"CREATE TABLE {stage_table} NOLOGGING AS SELECT * FROM {target_table} WHERE 1=0"]
    if norm in {"mssql", "sybase"}:
        return [f"SELECT TOP 0 * INTO {stage_table} FROM {target_table}"]
    if norm == "sqlanywhere":
        return [f"SELECT * INTO {stage_table} FROM {target_table} WHERE 1=0"]
    if norm == "db2":
        return [f"CREATE TABLE {stage_table} AS (SELECT * FROM {target_table}) WITH NO DATA"]
    if norm == "bigquery":
        return [f"CREATE TABLE IF NOT EXISTS {stage_table} LIKE {target_table}"]
    return [f"CREATE TABLE IF NOT EXISTS {stage_table} AS SELECT * FROM {target_table} WHERE 1=0"]


def build_stage_merge_sql(
    dialect: str,
    target_table: str,
    stage_table: str,
    columns: Sequence[str],
    *,
    conflict_keys: Sequence[str] | None = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: Sequence[str] | None = None,
    type_casts: Mapping[str, str] | None = None,
) -> str:
    """Build dialect-specific server-side ``INSERT ... SELECT`` or ``MERGE`` SQL from ``stage_table`` into ``target_table``."""
    norm = normalize_storage_dialect(dialect)
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
        set_clause = ", ".join(f"{col} = stg.{col}" for col in update_columns)
        return (
            f"INSERT INTO {target_table} ({col_list}) "
            f"SELECT {select_list} FROM {stage_table} AS stg "
            f"ON DUPLICATE KEY UPDATE {set_clause}"
        )

    on_clause = " AND ".join(f"tgt.{key} = stg.{key}" for key in conflict_keys)
    if norm in {"sybase", "teradata"}:
        stage_values = ", ".join(
            f"CAST(stg.{col} AS {type_casts[col]})" if (type_casts and col in type_casts) else f"stg.{col}"
            for col in columns
        )
        insert_sql = (
            f"INSERT INTO {target_table} ({col_list}) SELECT {stage_values} FROM {stage_table} stg "
            f"WHERE NOT EXISTS (SELECT 1 FROM {target_table} tgt WHERE {on_clause})"
        )
        if on_conflict == "nothing" or not update_columns:
            return insert_sql
        if norm == "teradata":
            src_assignments = ", ".join(
                f"{col} = CAST(src.{col} AS {type_casts[col]})"
                if (type_casts and col in type_casts)
                else f"{col} = src.{col}"
                for col in update_columns
            )
            where_clause = " AND ".join(f"tgt.{key} = src.{key}" for key in conflict_keys)
            return f"UPDATE tgt FROM {target_table} tgt, {stage_table} src SET {src_assignments} WHERE {where_clause}; {insert_sql}"
        assignments = ", ".join(
            f"{col} = CAST(stg.{col} AS {type_casts[col]})"
            if (type_casts and col in type_casts)
            else f"{col} = stg.{col}"
            for col in update_columns
        )
        return (
            f"UPDATE {target_table} SET {assignments} "
            f"FROM {target_table} tgt, {stage_table} stg WHERE {on_clause}; {insert_sql}"
        )
    matched_clause = ""
    if on_conflict == "update" and update_columns:
        set_pairs = ", ".join(
            f"tgt.{col} = CAST(stg.{col} AS {type_casts[col]})"
            if (type_casts and col in type_casts)
            else f"tgt.{col} = stg.{col}"
            for col in update_columns
        )
        matched_clause = f" WHEN MATCHED THEN UPDATE SET {set_pairs}"
    stage_vals = ", ".join(
        f"CAST(stg.{col} AS {type_casts[col]})" if (type_casts and col in type_casts) else f"stg.{col}"
        for col in columns
    )
    not_matched_clause = f" WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({stage_vals})"
    terminator = ";" if norm == "mssql" else ""
    return f"MERGE INTO {target_table} tgt USING {stage_table} stg ON ({on_clause}){matched_clause}{not_matched_clause}{terminator}"


def build_drop_stage_table_sql(dialect: str, stage_table: str) -> str:
    """Build SQL statement to drop a staging table."""
    norm = normalize_storage_dialect(dialect)
    if norm in {"oracle", "db2", "sybase", "teradata"}:
        return f"DROP TABLE {stage_table}"
    return f"DROP TABLE IF EXISTS {stage_table}"
