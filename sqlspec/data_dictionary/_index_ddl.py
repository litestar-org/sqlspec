"""Compiled secondary-index drop and rebuild helpers for bulk loading."""

import contextlib
import re
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal, Self, final

from sqlspec.data_dictionary._registry import normalize_adapter_dialect_name
from sqlspec.utils.text import quote_identifier, split_qualified_identifier

if TYPE_CHECKING:
    from types import TracebackType

__all__ = (
    "AsyncWithoutIndexesContext",
    "SyncWithoutIndexesContext",
    "build_create_index_sql",
    "build_drop_index_sql",
    "filter_droppable_table_indexes",
)

_MYSQL_FAMILY_DIALECTS: Final[frozenset[str]] = frozenset({"mysql", "mariadb"})
_SAP_DIALECTS: Final[frozenset[str]] = frozenset({"sybase", "sqlanywhere"})
_CREATE_UNIQUE_INDEX_RE: Final = re.compile(r"^\s*CREATE\s+UNIQUE\s+INDEX\s+(?!CONCURRENTLY\b)", re.IGNORECASE)
_CREATE_INDEX_RE: Final = re.compile(r"^\s*CREATE\s+INDEX\s+(?!CONCURRENTLY\b)", re.IGNORECASE)


def _canonical_index_dialect(dialect: str | None) -> str:
    """Normalize an adapter or dialect string for index DDL generation."""
    return normalize_adapter_dialect_name(dialect)


def _teradata_index_column_list(index_meta: Mapping[str, Any]) -> str:
    """Return the comma-separated key columns used to address an unnamed Teradata index."""
    cols = index_meta.get("columns") or index_meta.get("column_names") or []
    if isinstance(cols, str):
        return cols
    if isinstance(cols, Sequence):
        return ", ".join(str(c) for c in cols if c)
    return ""


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
    if norm == "bigquery":
        return None
    idx_name = str(index_meta.get("index_name") or index_meta.get("name") or "").strip()
    if not idx_name:
        return None
    resolved_schema = schema or (str(index_meta.get("schema_name") or "").strip() or None)
    if norm == "teradata":
        parts = split_qualified_identifier(table)
        if len(parts) == 1 and resolved_schema:
            parts = (*split_qualified_identifier(resolved_schema), *parts)
        qualified_table = ".".join(quote_identifier(part) for part in parts)
        if index_meta.get("catalog_name_generated"):
            column_list = _teradata_index_column_list(index_meta)
            return f"DROP INDEX ({column_list}) ON {qualified_table}" if column_list else None
        return f"DROP INDEX {quote_identifier(idx_name)} ON {qualified_table}"
    if norm in _SAP_DIALECTS:
        table_parts = split_qualified_identifier(table)
        if len(table_parts) == 1 and resolved_schema:
            table_parts = (*split_qualified_identifier(resolved_schema), *table_parts)
        quoted_parts = [quote_identifier(part) for part in table_parts]
        quoted_parts.append(quote_identifier(idx_name))
        return f"DROP INDEX {'.'.join(quoted_parts)}"
    if norm == "mssql":
        qualified_table = f"[{resolved_schema}].[{table}]" if resolved_schema and "." not in table else f"[{table}]"
        return f"DROP INDEX [{idx_name}] ON {qualified_table}"
    if norm in _MYSQL_FAMILY_DIALECTS:
        qualified_table = f"`{resolved_schema}`.`{table}`" if resolved_schema and "." not in table else f"`{table}`"
        return f"DROP INDEX `{idx_name}` ON {qualified_table}"
    if norm in {"oracle", "db2"}:
        qualified_idx = f'"{resolved_schema}"."{idx_name}"' if resolved_schema else f'"{idx_name}"'
        return f"DROP INDEX {qualified_idx}"
    if norm == "spanner":
        return f"DROP INDEX {idx_name}"
    qualified_idx = f'"{resolved_schema}"."{idx_name}"' if resolved_schema else f'"{idx_name}"'
    return f"DROP INDEX IF EXISTS {qualified_idx}"


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
        if norm == "teradata":
            parts = split_qualified_identifier(table)
            if len(parts) == 1 and resolved_schema:
                parts = (*split_qualified_identifier(resolved_schema), *parts)
            qualified_table = ".".join(quote_identifier(part) for part in parts)
            clean_cols = re.sub(r"\s+(?:ASC|DESC)(?=\s*(?:,|$))", "", col_list, flags=re.IGNORECASE)
            if index_meta.get("catalog_name_generated"):
                return f"CREATE {unique_kw}INDEX ({clean_cols}) ON {qualified_table}"
            return f"CREATE {unique_kw}INDEX {quote_identifier(idx_name)} ({clean_cols}) ON {qualified_table}"
        if norm in _SAP_DIALECTS:
            table_parts = split_qualified_identifier(table)
            if len(table_parts) == 1 and resolved_schema:
                table_parts = (*split_qualified_identifier(resolved_schema), *table_parts)
            qualified_table = ".".join(quote_identifier(part) for part in table_parts)
            quoted_index = quote_identifier(idx_name)
            sql_text = f"CREATE {unique_kw}INDEX {quoted_index} ON {qualified_table} ({col_list})"
        elif norm == "mssql":
            qualified_table = f"[{resolved_schema}].[{table}]" if resolved_schema and "." not in table else f"[{table}]"
            sql_text = f"CREATE {unique_kw}INDEX [{idx_name}] ON {qualified_table} ({col_list})"
        elif norm in _MYSQL_FAMILY_DIALECTS:
            qualified_table = f"`{resolved_schema}`.`{table}`" if resolved_schema and "." not in table else f"`{table}`"
            sql_text = f"CREATE {unique_kw}INDEX `{idx_name}` ON {qualified_table} ({col_list})"
        else:
            qualified_table = f'"{resolved_schema}"."{table}"' if resolved_schema and "." not in table else table
            sql_text = f'CREATE {unique_kw}INDEX "{idx_name}" ON {qualified_table} ({col_list})'

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
                table_name,
                schema=schema_name,
                concurrent=self._concurrent,
                in_transaction=in_tx,
                ddl_override=ddl_override,
            )
            drop_sql = build_drop_index_sql(dialect, idx, table_name, schema=schema_name)
            if drop_sql and create_sql:
                try:
                    self._driver.execute(drop_sql)
                except Exception:
                    self._restore_dropped_indexes()
                    raise
                self._rebuild_statements.append(create_sql)
        return self

    def _restore_dropped_indexes(self) -> None:
        """Recreate indexes dropped before a later drop failed, keeping the original error."""
        for stmt in self._rebuild_statements:
            with contextlib.suppress(Exception):
                self._driver.execute(stmt)
        self._rebuild_statements.clear()

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
                table_name,
                schema=schema_name,
                concurrent=self._concurrent,
                in_transaction=in_tx,
                ddl_override=ddl_override,
            )
            drop_sql = build_drop_index_sql(dialect, idx, table_name, schema=schema_name)
            if drop_sql and create_sql:
                try:
                    await self._driver.execute(drop_sql)
                except Exception:
                    await self._restore_dropped_indexes()
                    raise
                self._rebuild_statements.append(create_sql)
        return self

    async def _restore_dropped_indexes(self) -> None:
        """Recreate indexes dropped before a later drop failed, keeping the original error."""
        for stmt in self._rebuild_statements:
            with contextlib.suppress(Exception):
                await self._driver.execute(stmt)
        self._rebuild_statements.clear()

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


def _split_schema_and_table(table: str, explicit_schema: str | None) -> tuple[str | None, str]:
    """Split an optional schema prefix and unquote identifier delimiters."""
    clean = table.strip()
    if explicit_schema is not None:
        return explicit_schema, clean.rsplit(".", 1)[-1].strip('"`[]')
    if "." in clean:
        schema_part, table_part = clean.split(".", 1)
        return schema_part.strip('"`[]'), table_part.strip('"`[]')
    return None, clean.strip('"`[]')
