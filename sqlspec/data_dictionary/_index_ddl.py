"""Compiled secondary-index drop and rebuild helpers for bulk loading."""

import contextlib
import inspect
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal, Self, final

from sqlspec.data_dictionary._registry import normalize_adapter_dialect_name
from sqlspec.exceptions import TransactionError
from sqlspec.utils.text import quote_identifier, split_qualified_identifier

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    "AsyncWithoutIndexesContext",
    "SyncWithoutIndexesContext",
    "build_create_index_sql",
    "build_drop_index_sql",
    "ensure_teradata_ddl_outside_transaction",
    "filter_droppable_table_indexes",
)

_MYSQL_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({"mysql", "mariadb"})
_BACKTICK_INDEX_DIALECTS: Final[frozenset[str]] = _MYSQL_FAMILY_DIALECTS | frozenset({"spanner"})
_SAP_DIALECTS: Final[frozenset[str]] = frozenset({"sybase", "sqlanywhere"})
_INDEXLESS_DIALECTS: Final[frozenset[str]] = frozenset({"bigquery", "snowflake"})
_MIN_QUOTED_INDEX_PART_LEN: Final[int] = 2
_DIRECTION_TOKEN_PARTS: Final[int] = 2
_CREATE_UNIQUE_INDEX_RE: Final = re.compile(r"^\s*CREATE\s+UNIQUE\s+INDEX\s+(?!CONCURRENTLY\b)", re.IGNORECASE)
_CREATE_INDEX_RE: Final = re.compile(r"^\s*CREATE\s+INDEX\s+(?!CONCURRENTLY\b)", re.IGNORECASE)


def filter_droppable_table_indexes(
    indexes: Sequence[Mapping[str, Any]], constraints: Sequence[Mapping[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """Filter data-dictionary index metadata to secondary indexes safe to drop and rebuild.

    Args:
        indexes: Index metadata rows returned by a driver's data dictionary.
        constraints: Optional constraint metadata rows used to exclude constraint-backed indexes.

    Returns:
        List of secondary index metadata dictionaries safe for temporary removal.
    """
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
    dialect: str, index_meta: Mapping[str, Any], table: str, schema: str | None = None
) -> str | None:
    """Build a dialect-appropriate DROP INDEX statement for a secondary index.

    Args:
        dialect: Target dialect or adapter name.
        index_meta: Index metadata dictionary.
        table: Target table name.
        schema: Optional schema or database name override.

    Returns:
        Dialect-specific ``DROP INDEX`` SQL string, or ``None`` when unsupported.
    """
    norm = _canonical_index_dialect(dialect)
    if norm in _INDEXLESS_DIALECTS:
        return None
    idx_name = str(index_meta.get("index_name") or index_meta.get("name") or "").strip()
    if not idx_name:
        return None
    table_parts = split_qualified_identifier(table, quote_chars='"`', allow_bracket_quotes=True)
    schema_parts = _resolve_index_schema_parts(table_parts, schema, index_meta)
    if norm == "teradata":
        qualified_table = _format_index_table_parts(table_parts, schema_parts, norm)
        if index_meta.get("catalog_name_generated"):
            cols = index_meta.get("columns") or index_meta.get("column_names") or []
            column_list = _format_index_columns(cols, norm, allow_direction=False)
            return f"DROP INDEX ({column_list}) ON {qualified_table}" if column_list else None
        return f"DROP INDEX {_quote_index_part(idx_name, norm)} ON {qualified_table}"
    if norm in _SAP_DIALECTS:
        qualified_table = _format_index_table_parts(table_parts, schema_parts, norm)
        return f"DROP INDEX {qualified_table}.{_quote_index_part(idx_name, norm)}"
    if norm == "mssql":
        qualified_table = _format_index_table_parts(table_parts, schema_parts, norm)
        return f"DROP INDEX {_quote_index_part(idx_name, norm)} ON {qualified_table}"
    if norm in _MYSQL_FAMILY_DIALECTS:
        qualified_table = _format_index_table_parts(table_parts, schema_parts, norm)
        return f"DROP INDEX {_quote_index_part(idx_name, norm)} ON {qualified_table}"
    if norm in {"oracle", "db2"}:
        return f"DROP INDEX {_format_qualified_index_parts(idx_name, schema_parts, norm)}"
    if norm == "spanner":
        return f"DROP INDEX {_quote_index_part(idx_name, norm)}"
    return f"DROP INDEX IF EXISTS {_format_qualified_index_parts(idx_name, schema_parts, norm)}"


def build_create_index_sql(
    dialect: str,
    index_meta: Mapping[str, Any],
    table: str,
    *,
    schema: str | None = None,
    concurrent: bool = False,
    in_transaction: bool = False,
    ddl_override: str | None = None,
) -> str | None:
    """Build or adapt a dialect-specific CREATE INDEX statement to restore a dropped index.

    Args:
        dialect: Target dialect or adapter name.
        index_meta: Index metadata dictionary.
        table: Target table name.
        schema: Optional schema or database name override.
        concurrent: Whether to emit ``CONCURRENTLY`` on PostgreSQL outside transactions.
        in_transaction: Whether the driver currently holds an active transaction.
        ddl_override: Explicit DDL string returned by ``data_dictionary.get_ddl``.

    Returns:
        Dialect-specific ``CREATE INDEX`` SQL string, or ``None`` when unsupported.
    """
    norm = _canonical_index_dialect(dialect)
    if norm in _INDEXLESS_DIALECTS:
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
        col_list = _format_index_columns(cols, norm, allow_direction=norm != "teradata")
        if not idx_name or not col_list:
            return None
        table_parts = split_qualified_identifier(table, quote_chars='"`', allow_bracket_quotes=True)
        schema_parts = _resolve_index_schema_parts(table_parts, schema, index_meta)
        is_unique = bool(index_meta.get("is_unique") or index_meta.get("unique"))
        unique_kw = "UNIQUE " if is_unique else ""
        qualified_table = _format_index_table_parts(table_parts, schema_parts, norm)
        if norm == "teradata":
            if index_meta.get("catalog_name_generated"):
                return f"CREATE {unique_kw}INDEX ({col_list}) ON {qualified_table}"
            return f"CREATE {unique_kw}INDEX {_quote_index_part(idx_name, norm)} ({col_list}) ON {qualified_table}"
        quoted_index = _quote_index_part(idx_name, norm)
        sql_text = f"CREATE {unique_kw}INDEX {quoted_index} ON {qualified_table} ({col_list})"

    if concurrent and not in_transaction and norm == "postgres":
        if _CREATE_UNIQUE_INDEX_RE.match(sql_text):
            sql_text = _CREATE_UNIQUE_INDEX_RE.sub("CREATE UNIQUE INDEX CONCURRENTLY ", sql_text, count=1)
        elif _CREATE_INDEX_RE.match(sql_text):
            sql_text = _CREATE_INDEX_RE.sub("CREATE INDEX CONCURRENTLY ", sql_text, count=1)
    return sql_text


@final
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
        dialect = _canonical_index_dialect(str(getattr(self._driver, "dialect", "") or ""))
        if dialect in _INDEXLESS_DIALECTS:
            return self
        ensure_teradata_ddl_outside_transaction(self._driver, "index drop and rebuild")
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
        ddl_table, ddl_schema = _resolve_ddl_target(self._table, self._schema, table_name, dialect)
        for idx in droppable:
            ddl_override: str | None = None
            idx_name = str(idx.get("index_name") or idx.get("name") or "").strip()
            if (
                idx_name
                and not idx.get("catalog_name_generated")
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
                ddl_table,
                schema=ddl_schema,
                concurrent=self._concurrent,
                in_transaction=in_tx,
                ddl_override=ddl_override,
            )
            drop_sql = build_drop_index_sql(dialect, idx, ddl_table, schema=ddl_schema)
            if drop_sql and create_sql:
                try:
                    self._driver.execute(drop_sql)
                except BaseException:
                    self._restore_dropped_indexes()
                    raise
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

    def _restore_dropped_indexes(self) -> None:
        """Recreate indexes dropped before a later drop failed, keeping the original error."""
        for stmt in self._rebuild_statements:
            with contextlib.suppress(Exception):
                self._driver.execute(stmt)
        self._rebuild_statements.clear()


@final
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
        dialect = _canonical_index_dialect(str(getattr(self._driver, "dialect", "") or ""))
        if dialect in _INDEXLESS_DIALECTS:
            return self
        ensure_teradata_ddl_outside_transaction(self._driver, "index drop and rebuild")
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
        in_tx = await _safe_driver_in_transaction_async(self._driver)
        ddl_table, ddl_schema = _resolve_ddl_target(self._table, self._schema, table_name, dialect)
        for idx in droppable:
            ddl_override: str | None = None
            idx_name = str(idx.get("index_name") or idx.get("name") or "").strip()
            if (
                idx_name
                and not idx.get("catalog_name_generated")
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
                ddl_table,
                schema=ddl_schema,
                concurrent=self._concurrent,
                in_transaction=in_tx,
                ddl_override=ddl_override,
            )
            drop_sql = build_drop_index_sql(dialect, idx, ddl_table, schema=ddl_schema)
            if drop_sql and create_sql:
                try:
                    await self._driver.execute(drop_sql)
                except BaseException:
                    await self._restore_dropped_indexes()
                    raise
                self._rebuild_statements.append(create_sql)
        return self

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: "TracebackType | None"
    ) -> Literal[False]:
        in_tx = await _safe_driver_in_transaction_async(self._driver)
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

    async def _restore_dropped_indexes(self) -> None:
        """Recreate indexes dropped before a later drop failed, keeping the original error."""
        for stmt in self._rebuild_statements:
            with contextlib.suppress(Exception):
                await self._driver.execute(stmt)
        self._rebuild_statements.clear()


def ensure_teradata_ddl_outside_transaction(driver: Any, operation: str) -> None:
    """Refuse a Teradata DDL step that would end a transaction the caller still owns.

    Teradata requires DDL to be the last statement of a transaction (error 3932) in both TERA
    and ANSI transaction modes, so running it inside an open transaction would either fail or
    commit the caller's pending work. Other dialects are not checked.

    Args:
        driver: Driver about to run the DDL step.
        operation: Description of the operation for the error message.

    Raises:
        TransactionError: When ``driver`` is a Teradata driver with an open transaction.
    """
    if _canonical_index_dialect(str(getattr(driver, "dialect", "") or "")) != "teradata":
        return
    if not _safe_driver_in_transaction(driver):
        return
    msg = (
        f"Teradata {operation} runs DDL, and Teradata requires DDL to be the last statement of a "
        "transaction (error 3932). Commit or roll back the open transaction, or use an autocommit "
        "session, before running it."
    )
    raise TransactionError(msg)


def _canonical_index_dialect(dialect: str | None) -> str:
    """Normalize an adapter or dialect string for index DDL generation."""
    return normalize_adapter_dialect_name(dialect)


def _quote_index_part(part: str, dialect: str) -> str:
    """Quote a single index, table, schema, or column identifier segment for ``dialect``."""
    stripped = part.strip()
    if len(stripped) >= _MIN_QUOTED_INDEX_PART_LEN:
        if stripped[0] == '"' and stripped[-1] == '"':
            clean = stripped[1:-1].replace('""', '"')
        elif stripped[0] == "`" and stripped[-1] == "`":
            clean = stripped[1:-1].replace("``", "`")
        elif stripped[0] == "[" and stripped[-1] == "]":
            clean = stripped[1:-1].replace("]]", "]")
        else:
            clean = stripped
    else:
        clean = stripped
    if not clean or "\x00" in clean:
        msg = "Index DDL identifier must not be empty or contain NUL"
        raise ValueError(msg)
    if dialect == "mssql":
        escaped = clean.replace("]", "]]")
        return f"[{escaped}]"
    if dialect in _BACKTICK_INDEX_DIALECTS:
        escaped = clean.replace("`", "``")
        return f"`{escaped}`"
    return quote_identifier(clean)


def _resolve_ddl_target(table: str, schema: str | None, table_name: str, dialect: str) -> tuple[str, str | None]:
    """Preserve unsplit table qualification unless an explicit schema override was given."""
    if schema is None:
        return table, None
    return _quote_index_part(table_name, dialect), schema


def _resolve_index_schema_parts(
    table_parts: tuple[str, ...], explicit_schema: str | None, index_meta: Mapping[str, Any]
) -> tuple[str, ...]:
    """Resolve schema qualifier segments without re-splitting catalog metadata names."""
    if len(table_parts) > 1:
        return table_parts[:-1]
    if explicit_schema is not None and explicit_schema.strip():
        schema_parts = split_qualified_identifier(explicit_schema, quote_chars='"`', allow_bracket_quotes=True)
        return schema_parts or (explicit_schema.strip(),)
    meta_schema = str(index_meta.get("schema_name") or "").strip()
    if meta_schema:
        return (meta_schema,)
    return ()


def _format_qualified_index_parts(idx_name: str, schema_parts: tuple[str, ...], dialect: str) -> str:
    """Format an index identifier prefixed by already-resolved qualifier segments."""
    if schema_parts:
        return ".".join(_quote_index_part(part, dialect) for part in (*schema_parts, idx_name))
    return _quote_index_part(idx_name, dialect)


def _format_index_table_parts(table_parts: tuple[str, ...], schema_parts: tuple[str, ...], dialect: str) -> str:
    """Format a qualified or unqualified table reference with dialect-escaped segments."""
    if not table_parts:
        msg = "Index DDL table name must not be empty"
        raise ValueError(msg)
    full_parts = table_parts if len(table_parts) > 1 or not schema_parts else (*schema_parts, *table_parts)
    return ".".join(_quote_index_part(part, dialect) for part in full_parts)


def _format_index_columns(cols: Any, dialect: str, *, allow_direction: bool = True) -> str:
    """Format index key columns with dialect-escaped names and optional ASC/DESC modifiers."""
    raw_items: list[str]
    if isinstance(cols, str):
        raw_items = [item for item in cols.split(",") if item.strip()]
    elif isinstance(cols, Sequence):
        raw_items = [str(col) for col in cols if col is not None and str(col).strip()]
    else:
        return ""
    formatted: list[str] = []
    for item in raw_items:
        stripped = item.strip()
        if not stripped:
            continue
        direction = ""
        tokens = stripped.rsplit(None, 1)
        if len(tokens) == _DIRECTION_TOKEN_PARTS and tokens[1].upper() in {"ASC", "DESC"}:
            col_name = tokens[0]
            direction = tokens[1].upper()
        else:
            col_name = stripped
        quoted_col = _quote_index_part(col_name, dialect)
        formatted.append(f"{quoted_col} {direction}" if (direction and allow_direction) else quoted_col)
    return ", ".join(formatted)


def _extract_metadata_rows(result: Any) -> list[dict[str, Any]]:
    """Extract normalized dictionary rows from a data-dictionary result payload."""
    if result is None:
        return []
    data = getattr(result, "data", result)
    if isinstance(data, list):
        return [dict(item) if isinstance(item, Mapping) else {} for item in data]
    if isinstance(data, Sequence):
        return [dict(item) if isinstance(item, Mapping) else {} for item in data]
    return []


def _extract_ddl_text(result: Any) -> str | None:
    """Extract a raw SQL DDL string from a ``get_ddl`` response."""
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
    """Return whether ``driver`` reports an active transaction without raising."""
    checker = getattr(driver, "is_in_transaction", None)
    if callable(checker):
        with contextlib.suppress(Exception):
            return bool(checker())
    conn_checker = getattr(driver, "_connection_in_transaction", None)
    if callable(conn_checker):
        with contextlib.suppress(Exception):
            return bool(conn_checker())
    return False


async def _safe_driver_in_transaction_async(driver: Any) -> bool:
    """Return whether an async ``driver`` reports an active transaction without raising."""
    for method_name in ("is_in_transaction", "_transaction_open", "_connection_in_transaction"):
        checker = getattr(driver, method_name, None)
        if not callable(checker):
            continue
        with contextlib.suppress(Exception):
            res = checker()
            if inspect.isawaitable(res):
                res = await res
            return bool(res)
    return False


def _split_schema_and_table(table: str, explicit_schema: str | None) -> tuple[str | None, str]:
    """Split an optional schema prefix and unquote identifier delimiters."""
    table_parts = split_qualified_identifier(table, quote_chars='"`', allow_bracket_quotes=True)
    if explicit_schema is not None:
        schema_parts = split_qualified_identifier(explicit_schema, quote_chars='"`', allow_bracket_quotes=True)
        resolved_schema = ".".join(schema_parts) if schema_parts else explicit_schema.strip()
        table_name = table_parts[-1] if table_parts else ""
        return resolved_schema, table_name
    if len(table_parts) > 1:
        return ".".join(table_parts[:-1]), table_parts[-1]
    return None, (table_parts[0] if table_parts else "")
