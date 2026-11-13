"""Compiled staging-table, checkpoint-ledger, and worker thread-budget SQL builders."""

import os
import re
from collections.abc import Mapping, Sequence
from typing import Any, Final, Literal

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from sqlspec.data_dictionary._registry import normalize_adapter_dialect_name
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.arrow_helpers import sql_type_from_arrow_type
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.uuids import uuid4

__all__ = (
    "SAP_DIALECTS",
    "build_checkpoint_insert_sql",
    "build_checkpoint_select_sql",
    "build_checkpoint_table_ddl",
    "build_create_table_from_arrow_schema",
    "build_drop_stage_table_sql",
    "build_stage_merge_sql",
    "build_stage_table_ddl",
    "build_teradata_checkpoint_probe",
    "compute_worker_thread_budget",
    "format_storage_identifier",
    "normalize_storage_dialect",
    "quote_dialect_column",
    "resolve_stage_table_name",
    "validate_sql_cast_type",
)

_PG_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({"postgres", "cockroachdb", "spangres"})
_ON_CONFLICT_DIALECTS: Final[frozenset[str]] = _PG_FAMILY_DIALECTS | frozenset({"duckdb", "sqlite"})
_MYSQL_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({"mysql", "mariadb"})
_BACKTICK_DIALECTS: Final[frozenset[str]] = _MYSQL_FAMILY_DIALECTS | frozenset({"bigquery", "spanner"})
_BRACKET_DIALECTS: Final[frozenset[str]] = frozenset({"mssql", "tsql", "sybase"})
SAP_DIALECTS: Final[frozenset[str]] = frozenset({"sybase", "sqlanywhere"})
_QUALIFIED_TABLE_PARTS: Final[int] = 2
_MAX_QUALIFIED_IDENTIFIER_PARTS: Final[int] = 3
_MIN_QUOTED_LEN: Final[int] = 2
_BARE_STORAGE_IDENTIFIER_RE: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_QUALIFIED_PART_TOKEN_RE: Final = re.compile(r'"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[(?:[^\]]|\]\])*\]|[^.]+')
_SAFE_UDT_CAST_RE: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_$. ]*(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?$")
_RESERVED_STORAGE_KEYWORDS: Final[frozenset[str]] = frozenset({
    "select",
    "from",
    "where",
    "table",
    "drop",
    "delete",
    "insert",
    "update",
    "merge",
    "into",
    "values",
    "order",
    "group",
    "join",
    "on",
    "as",
    "and",
    "or",
    "not",
    "null",
    "create",
    "index",
})
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


def quote_dialect_column(column: str, dialect: str | None = None) -> str:
    """Quote a single column or identifier component using dialect-native delimiters.

    Args:
        column: Column or identifier segment.
        dialect: Optional target SQL dialect or adapter name.

    Returns:
        Quoted and escaped SQL identifier token.

    Raises:
        ValueError: When ``column`` is empty or contains a NUL character.
    """
    if not isinstance(column, str) or not column.strip() or "\x00" in column:
        msg = "Storage column identifier must not be empty or contain NUL"
        raise ValueError(msg)
    raw, _ = _unwrap_identifier_delimiters(column.strip())
    if not raw or "\x00" in raw:
        msg = "Storage column identifier must not be empty or contain NUL"
        raise ValueError(msg)
    norm = normalize_storage_dialect(dialect) if dialect else ""
    if norm in _BACKTICK_DIALECTS:
        escaped = raw.replace("`", "``")
        return f"`{escaped}`"
    if norm in _BRACKET_DIALECTS:
        escaped = raw.replace("]", "]]")
        return f"[{escaped}]"
    return quote_identifier(raw)


def format_storage_identifier(identifier: str, dialect: str | None = None, *, allow_qualified: bool = False) -> str:
    """Render identifiers safely, preserving explicit quotes and bare engine folding.

    Args:
        identifier: A column or table identifier.
        dialect: Optional target SQL dialect for native delimiter selection.
        allow_qualified: Whether dots separate schema and table components.

    Returns:
        Safely rendered SQL identifier text.

    Raises:
        ValueError: When an identifier is empty, contains NUL, or has invalid dot segments.
    """
    if not isinstance(identifier, str) or not identifier.strip() or "\x00" in identifier:
        msg = "Storage identifier must not be empty or contain NUL"
        raise ValueError(msg)
    stripped = identifier.strip()
    if allow_qualified:
        parts = _split_storage_identifier_tokens(stripped)
        if len(parts) > _MAX_QUALIFIED_IDENTIFIER_PARTS:
            msg = "Storage identifier exceeds maximum qualification depth"
            raise ValueError(msg)
    else:
        parts = [stripped]
    rendered: list[str] = []
    for component in parts:
        part = component.strip()
        if not part:
            msg = "Storage identifier component must not be empty"
            raise ValueError(msg)
        raw, was_quoted = _unwrap_identifier_delimiters(part)
        if not raw:
            msg = "Storage identifier component must not be empty"
            raise ValueError(msg)
        if (
            not was_quoted
            and _BARE_STORAGE_IDENTIFIER_RE.fullmatch(raw)
            and raw.lower() not in _RESERVED_STORAGE_KEYWORDS
        ):
            rendered.append(raw)
        else:
            rendered.append(quote_dialect_column(raw, dialect=dialect))
    return ".".join(rendered)


def validate_sql_cast_type(type_sql: str, dialect: str | None = None) -> str:
    """Validate a SQL data type expression used in staged ``CAST`` projections.

    Args:
        type_sql: Candidate SQL type text (such as ``INTEGER``, ``DECIMAL(18, 4)``, ``JSONB``).
        dialect: Optional target SQL dialect.

    Returns:
        Canonical validated SQL data type text.

    Raises:
        ValueError: If ``type_sql`` is empty, malformed, or contains non-type SQL constructs.
    """
    if not isinstance(type_sql, str):
        msg = "SQL cast type must be a non-empty string"
        raise TypeError(msg)
    stripped = type_sql.strip()
    if (
        not stripped
        or "\x00" in stripped
        or ";" in stripped
        or "--" in stripped
        or "/*" in stripped
        or "*/" in stripped
        or "'" in stripped
        or '"' in stripped
        or "`" in stripped
        or "[" in stripped
        or "]" in stripped
    ):
        msg = f"Invalid SQL cast type: {type_sql!r}"
        raise ValueError(msg)
    depth = 0
    for ch in stripped:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                msg = f"Invalid SQL cast type: {type_sql!r}"
                raise ValueError(msg)
    if depth != 0:
        msg = f"Invalid SQL cast type: {type_sql!r}"
        raise ValueError(msg)

    norm = normalize_storage_dialect(dialect) if dialect else ""
    cast_dialect = _sqlglot_cast_dialect(norm)
    try:
        parsed_type = exp.DataType.build(stripped, dialect=cast_dialect, udt=True)
        statements = sqlglot.parse(f"SELECT CAST(x AS {stripped})", read=cast_dialect)
    except SqlglotError as exc:
        msg = f"Invalid SQL cast type: {type_sql!r}"
        raise ValueError(msg) from exc

    stmt = statements[0] if len(statements) == 1 else None
    if not isinstance(stmt, exp.Select) or len(stmt.expressions) != 1:
        msg = f"Invalid SQL cast type: {type_sql!r}"
        raise ValueError(msg)
    if any(val for key, val in stmt.args.items() if key != "expressions"):
        msg = f"Invalid SQL cast type: {type_sql!r}"
        raise ValueError(msg)
    projection = stmt.expressions[0]
    if (
        not isinstance(projection, exp.Cast)
        or not isinstance(projection.this, exp.Column)
        or projection.this.name != "x"
        or not isinstance(projection.to, exp.DataType)
        or projection.to.find(exp.Query, exp.Subquery, exp.DDL, exp.DML, exp.Command) is not None
        or any(node.comments for node in stmt.walk())
    ):
        msg = f"Invalid SQL cast type: {type_sql!r}"
        raise ValueError(msg)

    if parsed_type.this in {
        exp.DataType.Type.UNKNOWN,
        exp.DataType.Type.USERDEFINED,
    } and not _SAFE_UDT_CAST_RE.fullmatch(stripped):
        msg = f"Invalid SQL cast type: {type_sql!r}"
        raise ValueError(msg)

    return parsed_type.sql(dialect=cast_dialect, copy=False)


def build_create_table_from_arrow_schema(
    table_name: str, arrow_schema: Any, *, dialect: str = "snowflake", if_not_exists: bool = True
) -> str:
    """Build CREATE TABLE from a PyArrow schema without interpolating values.

    Args:
        table_name: Target table, optionally qualified with a schema.
        arrow_schema: PyArrow schema or table with at least one field.
        dialect: Target SQL dialect for field type mapping.
        if_not_exists: Whether to include the idempotent creation clause.

    Returns:
        CREATE TABLE SQL without a trailing semicolon.

    Raises:
        ValueError: When the table name or schema is empty.
    """
    schema = getattr(arrow_schema, "schema", arrow_schema)
    if not len(schema):
        msg = "Arrow schema must contain at least one field"
        raise ValueError(msg)
    table = format_storage_identifier(table_name, dialect=dialect, allow_qualified=True)
    fields = ", ".join(
        f"{format_storage_identifier(field.name, dialect=dialect)} {sql_type_from_arrow_type(field.type, dialect)}"
        for field in schema
    )
    conditional = " IF NOT EXISTS" if if_not_exists else ""
    return f"CREATE TABLE{conditional} {table} ({fields})"


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
    formatted_table = format_storage_identifier(checkpoint_table, dialect=norm, allow_qualified=True)
    if norm == "teradata":
        return (
            f"CREATE MULTISET TABLE {formatted_table} ("
            "phase VARCHAR(128) NOT NULL, shard_uri VARCHAR(1024) NOT NULL, "
            "rows_loaded BIGINT NOT NULL, bytes_processed BIGINT NOT NULL, duration_s DOUBLE PRECISION NOT NULL, "
            "completed_at TIMESTAMP(6) WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP(6)) "
            "UNIQUE PRIMARY INDEX (phase, shard_uri)"
        )
    if norm == "sybase":
        create_sql = (
            f"CREATE TABLE {formatted_table} ("
            "phase VARCHAR(128) NOT NULL, "
            "shard_uri VARCHAR(1024) NOT NULL, "
            "rows_loaded BIGINT NOT NULL, "
            "bytes_processed BIGINT NOT NULL, "
            "duration_s FLOAT NOT NULL, "
            "completed_at BIGDATETIME DEFAULT GETDATE(), "
            "PRIMARY KEY (phase, shard_uri))"
        )
        object_name = formatted_table.replace("'", "''")
        escaped_create = create_sql.replace("'", "''")
        return f"IF OBJECT_ID('{object_name}') IS NULL EXECUTE ('{escaped_create}')"
    if norm == "mssql":
        object_name = formatted_table.replace("'", "''")
        return (
            f"IF OBJECT_ID('{object_name}', 'U') IS NULL "
            f"CREATE TABLE {formatted_table} ("
            "phase VARCHAR(128) NOT NULL, "
            "shard_uri VARCHAR(1024) NOT NULL, "
            "rows_loaded BIGINT NOT NULL, "
            "bytes_processed BIGINT NOT NULL, "
            "duration_s FLOAT NOT NULL, "
            "completed_at DATETIME2 DEFAULT CURRENT_TIMESTAMP, "
            "PRIMARY KEY (phase, shard_uri))"
        )
    return (
        f"CREATE TABLE IF NOT EXISTS {formatted_table} ("
        "phase VARCHAR(128) NOT NULL, "
        "shard_uri VARCHAR(1024) NOT NULL, "
        "rows_loaded BIGINT NOT NULL, "
        "bytes_processed BIGINT NOT NULL, "
        "duration_s DOUBLE PRECISION NOT NULL, "
        "completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
        "PRIMARY KEY (phase, shard_uri))"
    )


def build_checkpoint_select_sql(checkpoint_table: str, dialect: str | None = None) -> str:
    """Build SQL to select already-completed shard URIs for a given checkpoint phase."""
    norm = normalize_storage_dialect(dialect) if dialect else None
    formatted_table = format_storage_identifier(checkpoint_table, dialect=norm, allow_qualified=True)
    return f"SELECT shard_uri FROM {formatted_table} WHERE phase = :phase"


def build_checkpoint_insert_sql(dialect: str, checkpoint_table: str) -> str:
    """Build SQL to record a completed shard in the checkpoint table."""
    norm = normalize_storage_dialect(dialect)
    formatted_table = format_storage_identifier(checkpoint_table, dialect=norm, allow_qualified=True)
    if norm == "teradata":
        return (
            f"MERGE INTO {formatted_table} AS tgt USING (SELECT "
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
            f"INSERT INTO {formatted_table} (phase, shard_uri, rows_loaded, bytes_processed, duration_s) "
            "VALUES (:phase, :shard_uri, :rows_loaded, :bytes_processed, :duration_s) "
            "ON CONFLICT (phase, shard_uri) DO UPDATE SET "
            "rows_loaded = EXCLUDED.rows_loaded, "
            "bytes_processed = EXCLUDED.bytes_processed, "
            "duration_s = EXCLUDED.duration_s"
        )
    if norm in _MYSQL_FAMILY_DIALECTS:
        return (
            f"INSERT INTO {formatted_table} (phase, shard_uri, rows_loaded, bytes_processed, duration_s) "
            "VALUES (:phase, :shard_uri, :rows_loaded, :bytes_processed, :duration_s) "
            "ON DUPLICATE KEY UPDATE "
            "rows_loaded = VALUES(rows_loaded), "
            "bytes_processed = VALUES(bytes_processed), "
            "duration_s = VALUES(duration_s)"
        )
    return (
        f"INSERT INTO {formatted_table} (phase, shard_uri, rows_loaded, bytes_processed, duration_s) "
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
    norm_dialect = normalize_storage_dialect(dialect)
    if isinstance(stage_table, str):
        stripped = stage_table.strip()
        if not stripped:
            return None
        format_storage_identifier(stripped, dialect=norm_dialect, allow_qualified=True)
        return stripped
    if stage_table is False:
        return None
    needs_stage = stage_table is True or (bool(conflict_keys) and norm_dialect != "spanner")
    if not needs_stage:
        return None
    format_storage_identifier(target_table, dialect=norm_dialect, allow_qualified=True)
    suffix = uuid4().hex[:8]
    tokens = _split_storage_identifier_tokens(target_table.strip())
    raw_base, base_was_quoted = _unwrap_identifier_delimiters(tokens[-1].strip())
    stage_leaf_raw = f"{raw_base}_stage_{suffix}"
    formatted_leaf = (
        quote_dialect_column(stage_leaf_raw, dialect=norm_dialect)
        if base_was_quoted or not _BARE_STORAGE_IDENTIFIER_RE.fullmatch(stage_leaf_raw)
        else stage_leaf_raw
    )
    if len(tokens) > 1:
        prefix = format_storage_identifier(".".join(tokens[:-1]), dialect=norm_dialect, allow_qualified=True)
        return f"{prefix}.{formatted_leaf}"
    return formatted_leaf


def build_stage_table_ddl(
    dialect: str, target_table: str, stage_table: str, *, multi_worker: bool = False
) -> list[str]:
    """Build dialect-specific DDL statements to create an empty staging table matching ``target_table``."""
    norm = normalize_storage_dialect(dialect)
    formatted_target = format_storage_identifier(target_table, dialect=norm, allow_qualified=True)
    formatted_stage = format_storage_identifier(stage_table, dialect=norm, allow_qualified=True)
    has_schema = len(_split_storage_identifier_tokens(stage_table.strip())) > 1
    if norm == "teradata":
        return [f"CREATE MULTISET TABLE {formatted_stage} AS (SELECT * FROM {formatted_target}) WITH NO DATA"]
    if norm in _PG_FAMILY_DIALECTS:
        return [f"CREATE UNLOGGED TABLE IF NOT EXISTS {formatted_stage} (LIKE {formatted_target} INCLUDING DEFAULTS)"]
    if norm in {"duckdb", "sqlite"}:
        if multi_worker or has_schema:
            return [f"CREATE TABLE IF NOT EXISTS {formatted_stage} AS SELECT * FROM {formatted_target} WHERE 1=0"]
        return [f"CREATE TEMP TABLE IF NOT EXISTS {formatted_stage} AS SELECT * FROM {formatted_target} WHERE 1=0"]
    if norm in _MYSQL_FAMILY_DIALECTS:
        if multi_worker:
            return [f"CREATE TABLE IF NOT EXISTS {formatted_stage} LIKE {formatted_target}"]
        return [f"CREATE TEMPORARY TABLE IF NOT EXISTS {formatted_stage} LIKE {formatted_target}"]
    if norm == "oracle":
        return [f"CREATE TABLE {formatted_stage} NOLOGGING AS SELECT * FROM {formatted_target} WHERE 1=0"]
    if norm in {"mssql", "sybase"}:
        return [f"SELECT TOP 0 * INTO {formatted_stage} FROM {formatted_target}"]
    if norm == "sqlanywhere":
        return [f"SELECT * INTO {formatted_stage} FROM {formatted_target} WHERE 1=0"]
    if norm == "db2":
        return [f"CREATE TABLE {formatted_stage} AS (SELECT * FROM {formatted_target}) WITH NO DATA"]
    if norm == "bigquery":
        return [f"CREATE TABLE IF NOT EXISTS {formatted_stage} LIKE {formatted_target}"]
    return [f"CREATE TABLE IF NOT EXISTS {formatted_stage} AS SELECT * FROM {formatted_target} WHERE 1=0"]


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
    formatted_target = format_storage_identifier(target_table, dialect=norm, allow_qualified=True)
    formatted_stage = format_storage_identifier(stage_table, dialect=norm, allow_qualified=True)
    quoted_columns = [quote_dialect_column(column, dialect=norm) for column in columns]
    quoted_keys = [quote_dialect_column(column, dialect=norm) for column in conflict_keys] if conflict_keys else None
    quoted_excluded = (
        [quote_dialect_column(column, dialect=norm) for column in exclude_update_columns]
        if exclude_update_columns
        else None
    )
    validated_casts = (
        {
            quote_dialect_column(column, dialect=norm): validate_sql_cast_type(cast_type, dialect=norm)
            for column, cast_type in type_casts.items()
        }
        if type_casts
        else None
    )
    col_list = ", ".join(quoted_columns)
    select_exprs = [
        f"CAST({col} AS {validated_casts[col]})" if (validated_casts and col in validated_casts) else col
        for col in quoted_columns
    ]
    select_list = ", ".join(select_exprs)
    if not quoted_keys:
        return f"INSERT INTO {formatted_target} ({col_list}) SELECT {select_list} FROM {formatted_stage}"

    key_list = ", ".join(quoted_keys)
    excluded_set = set(quoted_keys) | set(quoted_excluded or ())
    update_columns = [col for col in quoted_columns if col not in excluded_set]

    if norm in _ON_CONFLICT_DIALECTS:
        if on_conflict == "nothing" or not update_columns:
            return (
                f"INSERT INTO {formatted_target} ({col_list}) "
                f"SELECT {select_list} FROM {formatted_stage} WHERE TRUE "
                f"ON CONFLICT ({key_list}) DO NOTHING"
            )
        set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
        return (
            f"INSERT INTO {formatted_target} ({col_list}) "
            f"SELECT {select_list} FROM {formatted_stage} WHERE TRUE "
            f"ON CONFLICT ({key_list}) DO UPDATE SET {set_clause}"
        )

    if norm in _MYSQL_FAMILY_DIALECTS:
        if on_conflict == "nothing" or not update_columns:
            return f"INSERT IGNORE INTO {formatted_target} ({col_list}) SELECT {select_list} FROM {formatted_stage}"
        set_clause = ", ".join(f"{col} = stg.{col}" for col in update_columns)
        return (
            f"INSERT INTO {formatted_target} ({col_list}) "
            f"SELECT {select_list} FROM {formatted_stage} AS stg "
            f"ON DUPLICATE KEY UPDATE {set_clause}"
        )

    on_clause = " AND ".join(f"tgt.{key} = stg.{key}" for key in quoted_keys)
    if norm in {"sybase", "teradata"}:
        stage_values = ", ".join(
            f"CAST(stg.{col} AS {validated_casts[col]})"
            if (validated_casts and col in validated_casts)
            else f"stg.{col}"
            for col in quoted_columns
        )
        insert_sql = (
            f"INSERT INTO {formatted_target} ({col_list}) SELECT {stage_values} FROM {formatted_stage} stg "
            f"WHERE NOT EXISTS (SELECT 1 FROM {formatted_target} tgt WHERE {on_clause})"
        )
        if on_conflict == "nothing" or not update_columns:
            return insert_sql
        if norm == "teradata":
            src_assignments = ", ".join(
                f"{col} = CAST(src.{col} AS {validated_casts[col]})"
                if (validated_casts and col in validated_casts)
                else f"{col} = src.{col}"
                for col in update_columns
            )
            where_clause = " AND ".join(f"tgt.{key} = src.{key}" for key in quoted_keys)
            return f"UPDATE tgt FROM {formatted_target} tgt, {formatted_stage} src SET {src_assignments} WHERE {where_clause}; {insert_sql}"
        assignments = ", ".join(
            f"{col} = CAST(stg.{col} AS {validated_casts[col]})"
            if (validated_casts and col in validated_casts)
            else f"{col} = stg.{col}"
            for col in update_columns
        )
        return (
            f"UPDATE {formatted_target} SET {assignments} "
            f"FROM {formatted_target} tgt, {formatted_stage} stg WHERE {on_clause}; {insert_sql}"
        )
    matched_clause = ""
    if on_conflict == "update" and update_columns:
        set_pairs = ", ".join(
            f"tgt.{col} = CAST(stg.{col} AS {validated_casts[col]})"
            if (validated_casts and col in validated_casts)
            else f"tgt.{col} = stg.{col}"
            for col in update_columns
        )
        matched_clause = f" WHEN MATCHED THEN UPDATE SET {set_pairs}"
    stage_vals = ", ".join(
        f"CAST(stg.{col} AS {validated_casts[col]})" if (validated_casts and col in validated_casts) else f"stg.{col}"
        for col in quoted_columns
    )
    not_matched_clause = f" WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({stage_vals})"
    terminator = ";" if norm == "mssql" else ""
    return f"MERGE INTO {formatted_target} tgt USING {formatted_stage} stg ON ({on_clause}){matched_clause}{not_matched_clause}{terminator}"


def build_drop_stage_table_sql(dialect: str, stage_table: str) -> str:
    """Build SQL statement to drop a staging table."""
    norm = normalize_storage_dialect(dialect)
    formatted_stage = format_storage_identifier(stage_table, dialect=norm, allow_qualified=True)
    if norm in {"oracle", "db2", "sybase", "teradata"}:
        return f"DROP TABLE {formatted_stage}"
    return f"DROP TABLE IF EXISTS {formatted_stage}"


def _unwrap_identifier_delimiters(token: str) -> tuple[str, bool]:
    """Strip matched outer SQL identifier delimiters and unescape doubled inner delimiters."""
    if len(token) >= _MIN_QUOTED_LEN:
        if token[0] == '"' and token[-1] == '"':
            return token[1:-1].replace('""', '"'), True
        if token[0] == "`" and token[-1] == "`":
            return token[1:-1].replace("``", "`"), True
        if token[0] == "[" and token[-1] == "]":
            return token[1:-1].replace("]]", "]"), True
    return token, False


def _split_storage_identifier_tokens(identifier: str) -> list[str]:
    """Split a qualified identifier on dots outside quoted tokens, rejecting empty segments."""
    if identifier.startswith(".") or identifier.endswith("."):
        msg = "Storage identifier component must not be empty"
        raise ValueError(msg)
    tokens: list[str] = []
    pos = 0
    length = len(identifier)
    for match in _QUALIFIED_PART_TOKEN_RE.finditer(identifier):
        if match.start() != pos:
            between = identifier[pos : match.start()]
            if between != ".":
                msg = "Storage identifier component must not be empty"
                raise ValueError(msg)
        token = match.group(0)
        if not token.strip():
            msg = "Storage identifier component must not be empty"
            raise ValueError(msg)
        tokens.append(token)
        pos = match.end()
    if pos != length or not tokens:
        msg = "Storage identifier component must not be empty"
        raise ValueError(msg)
    return tokens


def _sqlglot_cast_dialect(norm_dialect: str) -> str | None:
    """Map normalized storage dialect names to SQLGlot dialect names for type parsing."""
    if norm_dialect in {"cockroachdb", "spangres"}:
        return "postgres"
    if norm_dialect == "mariadb":
        return "mysql"
    if norm_dialect in {"mssql", "sybase"}:
        return "tsql"
    if norm_dialect == "teradata":
        return "vantage"
    if norm_dialect == "sqlanywhere":
        return None
    return norm_dialect or None
