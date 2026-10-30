"""SQLite dialect static configuration and reflection helpers."""

import re
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from sqlspec.data_dictionary import (
    DDL_PARENT_TABLE_CONTEXT_KEY,
    DDLResult,
    DependencyEdge,
    DependencyEdgeKind,
    DialectConfig,
    FeatureFlags,
    FeatureVersions,
    MetadataFidelity,
    MetadataSource,
    MetadataSupport,
    ObjectIdentity,
    VersionInfo,
    register_dialect,
)
from sqlspec.exceptions import SQLSpecError
from sqlspec.utils.text import quote_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "SQLITE_CONFIG",
    "SQLITE_FEATURE_FLAGS",
    "SQLITE_FEATURE_VERSIONS",
    "SQLITE_TYPE_MAPPINGS",
    "SQLITE_VERSION_PATTERN",
    "format_sqlite_schema_prefix",
    "list_sqlite_available_features",
    "normalize_sqlite_schema_name",
    "resolve_sqlite_json_type",
    "sqlite_check_constraints",
    "sqlite_constraint_metadata",
    "sqlite_generated_expressions",
    "sqlite_object_ddl_result",
    "sqlite_schema_ddl_results",
)


SQLITE_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")
_SQLITE_UNQUOTED_SCHEMA_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$-]*$")
_ASCII_CONTROL_MAX = 32
_ASCII_DEL = 127
_MIN_QUOTED_IDENTIFIER_LEN = 2

SQLITE_FEATURE_VERSIONS: "FeatureVersions" = {
    "supports_json": VersionInfo(3, 38, 0),
    "supports_returning": VersionInfo(3, 35, 0),
    "supports_upsert": VersionInfo(3, 24, 0),
    "supports_window_functions": VersionInfo(3, 25, 0),
    "supports_cte": VersionInfo(3, 8, 3),
}

SQLITE_FEATURE_FLAGS: "FeatureFlags" = {
    "supports_transactions": True,
    "supports_prepared_statements": True,
    "supports_schemas": False,
    "supports_arrays": False,
    "supports_uuid": False,
    "supports_for_update": False,
    "supports_skip_locked": False,
    "supports_on_conflict": True,
    "supports_update_from": True,
}

SQLITE_TYPE_MAPPINGS: dict[str, str] = {
    "uuid": "TEXT",
    "boolean": "INTEGER",
    "timestamp": "TIMESTAMP",
    "text": "TEXT",
    "blob": "BLOB",
    "json": "JSON",
    "integer": "INTEGER",
    "bigint": "INTEGER",
    "float": "REAL",
    "varchar": "TEXT",
}


SQLITE_CONFIG = DialectConfig(
    name="sqlite",
    feature_versions=SQLITE_FEATURE_VERSIONS,
    feature_flags=SQLITE_FEATURE_FLAGS,
    type_mappings=SQLITE_TYPE_MAPPINGS,
    version_pattern=SQLITE_VERSION_PATTERN,
)

register_dialect(SQLITE_CONFIG)


def resolve_sqlite_json_type(version_info: "VersionInfo | None") -> str:
    """Resolve the best SQLite JSON storage type for a database version."""
    json_version = SQLITE_CONFIG.get_feature_version("supports_json")
    if version_info and json_version and version_info >= json_version:
        return "JSON"
    return "TEXT"


def list_sqlite_available_features() -> "list[str]":
    """List SQLite data-dictionary feature flags."""
    features = set(SQLITE_CONFIG.feature_flags.keys()) | set(SQLITE_CONFIG.feature_versions.keys())
    return sorted(features)


def normalize_sqlite_schema_name(schema: "str | None") -> "str | None":
    """Validate and normalize a SQLite schema identifier into its exact catalog name.

    Args:
        schema: Attached database/schema name, optionally wrapped in identifier quotes, or None.

    Returns:
        Unquoted SQLite schema name, or None when ``schema`` is None.

    Raises:
        SQLSpecError: If ``schema`` is empty, contains NUL/control characters, or is not a single
            valid SQLite schema identifier.
    """
    if schema is None:
        return None
    cleaned = schema.strip()
    if not cleaned or "\x00" in cleaned or any(ord(ch) < _ASCII_CONTROL_MAX or ord(ch) == _ASCII_DEL for ch in cleaned):
        msg = f"Invalid SQLite schema identifier: {schema!r}"
        raise SQLSpecError(msg)
    if len(cleaned) >= _MIN_QUOTED_IDENTIFIER_LEN and cleaned[0] == cleaned[-1] == '"':
        inner = cleaned[1:-1]
        if '"' in inner.replace('""', ""):
            msg = f"Invalid SQLite schema identifier: {schema!r}"
            raise SQLSpecError(msg)
        cleaned = inner.replace('""', '"')
    elif len(cleaned) >= _MIN_QUOTED_IDENTIFIER_LEN and cleaned[0] == cleaned[-1] == "`":
        inner = cleaned[1:-1]
        if "`" in inner.replace("``", ""):
            msg = f"Invalid SQLite schema identifier: {schema!r}"
            raise SQLSpecError(msg)
        cleaned = inner.replace("``", "`")
    elif len(cleaned) >= _MIN_QUOTED_IDENTIFIER_LEN and cleaned[0] == "[" and cleaned[-1] == "]":
        inner = cleaned[1:-1]
        if "]" in inner.replace("]]", ""):
            msg = f"Invalid SQLite schema identifier: {schema!r}"
            raise SQLSpecError(msg)
        cleaned = inner.replace("]]", "]")
    elif _SQLITE_UNQUOTED_SCHEMA_PATTERN.fullmatch(cleaned) is None:
        msg = f"Invalid SQLite schema identifier: {schema!r}"
        raise SQLSpecError(msg)
    if not cleaned:
        msg = f"Invalid SQLite schema identifier: {schema!r}"
        raise SQLSpecError(msg)
    return cleaned


def format_sqlite_schema_prefix(schema: "str | None") -> str:
    """Validate and quote a SQLite schema identifier for ``{schema_prefix}sqlite_schema`` queries.

    SQLite does not allow database/schema qualifiers in ``FROM <schema>.sqlite_schema`` to be
    bound as SQL parameters. This helper enforces that ``schema`` is a single SQLite database
    identifier (never a dotted path, statement fragment, or control/NUL sequence) and returns it
    as a double-quoted identifier prefix with a trailing dot (``"schema".``).

    Args:
        schema: Attached database/schema name, optionally wrapped in identifier quotes, or None.

    Returns:
        Quoted ``"<schema>".`` prefix, or ``""`` when ``schema`` is None.

    Raises:
        SQLSpecError: If ``schema`` is empty, contains NUL/control characters, or is not a single
            valid SQLite schema identifier.
    """
    normalized = normalize_sqlite_schema_name(schema)
    if normalized is None:
        return ""
    return f"{quote_identifier(normalized)}."


def sqlite_object_ddl_result(
    rows: "Sequence[Mapping[str, Any]]", *, object_name: str, object_type: str, schema: "str | None"
) -> DDLResult:
    """Build a native DDL result from a ``ddl.sql`` ``by_object`` row.

    Args:
        rows: Rows returned by the ``by_object`` query.
        object_name: Exact object name.
        object_type: Requested object type.
        schema: Exact schema name.

    Returns:
        Native DDL result, or an unsupported result when the object has no stored SQL.
    """
    row = rows[0] if rows else None
    resolved_schema = row.get("schema_name") if row is not None else schema
    identity = ObjectIdentity(
        name=object_name,
        object_type=str(row.get("object_type") or object_type) if row is not None else object_type,
        schema=str(resolved_schema) if resolved_schema is not None else None,
        dialect="sqlite",
        source=MetadataSource.CATALOG,
    )
    ddl_value = row.get("ddl") if row is not None else None
    if ddl_value is None:
        return DDLResult.unsupported(identity, source=MetadataSource.CATALOG)
    return DDLResult(
        identity=identity,
        status=MetadataSupport.SUPPORTED,
        fidelity=MetadataFidelity.NATIVE,
        source=MetadataSource.CATALOG,
        ddl=_terminate_statement(str(ddl_value)),
    )


def sqlite_schema_ddl_results(rows: "Sequence[Mapping[str, Any]]", *, schema: "str | None") -> "list[DDLResult]":
    """Build native DDL results for every ``sqlite_schema`` object.

    Indexes and triggers carry an edge and a parent-table context entry for their table; views
    carry edges to the tables and views their SQL references.

    Args:
        rows: Rows returned by the ``ddl.sql`` ``by_schema`` query.
        schema: Exact schema name used for the query.

    Returns:
        Native DDL results in catalog order.
    """
    entries: list[tuple[ObjectIdentity, str, str | None]] = []
    relations: dict[str, ObjectIdentity] = {}
    for row in rows:
        object_name = str(row.get("object_name") or "")
        ddl_value = row.get("ddl")
        if not object_name or ddl_value is None:
            continue
        object_type = str(row.get("object_type") or "table").lower()
        row_schema = row.get("schema_name", schema)
        identity = ObjectIdentity(
            name=object_name,
            object_type=object_type,
            schema=str(row_schema) if row_schema is not None else None,
            dialect="sqlite",
            source=MetadataSource.CATALOG,
        )
        parent_value = row.get("table_name")
        entries.append((identity, _terminate_statement(str(ddl_value)), str(parent_value) if parent_value else None))
        if object_type in {"table", "view"}:
            relations[object_name.lower()] = identity

    results: list[DDLResult] = []
    for identity, ddl_text, parent_table in entries:
        edges: list[DependencyEdge] = []
        context: dict[str, object] = {}
        if identity.object_type in {"index", "trigger"} and parent_table is not None:
            context[DDL_PARENT_TABLE_CONTEXT_KEY] = parent_table
            parent_identity = relations.get(parent_table.lower())
            if parent_identity is not None:
                kind = (
                    DependencyEdgeKind.TRIGGER_TARGET
                    if identity.object_type == "trigger"
                    else DependencyEdgeKind.INDEX_EXPRESSION
                )
                edges.append(DependencyEdge(identity, parent_identity, kind, source=MetadataSource.CATALOG))
        elif identity.object_type == "view":
            edges.extend(
                DependencyEdge(identity, ref_identity, DependencyEdgeKind.VIEW_REFERENCE, source=MetadataSource.CATALOG)
                for ref_key, ref_identity in relations.items()
                if ref_key != identity.name.lower()
                and re.search(rf"(?<![\w$]){re.escape(ref_identity.name)}(?![\w$])", ddl_text, re.IGNORECASE)
            )
        results.append(
            DDLResult(
                identity=identity,
                status=MetadataSupport.SUPPORTED,
                fidelity=MetadataFidelity.NATIVE,
                source=MetadataSource.CATALOG,
                ddl=ddl_text,
                dependencies=tuple(edges),
                context=context,
            )
        )
    return results


def sqlite_constraint_metadata(rows: "Sequence[Mapping[str, Any]]") -> "list[dict[str, Any]]":
    """Group per-column ``constraints.sql`` rows into one row per PRIMARY KEY or UNIQUE constraint.

    SQLite does not retain constraint names, so ``constraint_name`` is None and generated DDL
    emits unnamed constraints.

    Args:
        rows: Per-column constraint rows ordered by table, index, and column sequence.

    Returns:
        Constraint rows with an ordered ``columns`` list.
    """
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("table_name")), str(row.get("index_name")))
        constraint = grouped.get(key)
        if constraint is None:
            constraint = {
                "schema_name": row.get("schema_name"),
                "table_name": row.get("table_name"),
                "constraint_name": None,
                "constraint_type": row.get("constraint_type"),
                "columns": [],
            }
            grouped[key] = constraint
        column_name = row.get("column_name")
        if column_name is not None:
            constraint["columns"].append(str(column_name))
    return list(grouped.values())


def sqlite_check_constraints(
    table_name: str, create_sql: "str | None", *, schema_name: "str | None" = None
) -> "tuple[list[dict[str, Any]], tuple[str, ...]]":
    """Extract column- and table-level CHECK constraints from a stored CREATE TABLE statement.

    SQLite exposes CHECK constraints only in ``sqlite_schema.sql``.

    Args:
        table_name: Table the statement creates.
        create_sql: Stored ``CREATE TABLE`` statement.
        schema_name: Schema name to report on each constraint row.

    Returns:
        CHECK constraint rows with ``check_clause`` text, and warnings for statements that
        could not be parsed.
    """
    table = _parse_create_table(create_sql)
    if isinstance(table, str):
        return ([], (f"CHECK constraints of {table_name} could not be read: {table}",))
    if table is None:
        return ([], ())
    rows: list[dict[str, Any]] = []
    schema = table.this
    definitions = schema.expressions if isinstance(schema, exp.Schema) else []
    for definition in definitions:
        if isinstance(definition, exp.ColumnDef):
            for constraint in definition.args.get("constraints") or ():
                kind = constraint.args.get("kind")
                if isinstance(kind, exp.CheckColumnConstraint):
                    name = constraint.args.get("this")
                    rows.append(_check_row(schema_name, table_name, name.name if name else None, kind, definition.name))
        elif isinstance(definition, exp.Constraint):
            rows.extend(
                _check_row(schema_name, table_name, definition.name or None, kind, None)
                for kind in definition.expressions
                if isinstance(kind, exp.CheckColumnConstraint)
            )
        elif isinstance(definition, exp.CheckColumnConstraint):
            rows.append(_check_row(schema_name, table_name, None, definition, None))
    return (rows, ())


def sqlite_generated_expressions(create_sql: "str | None") -> "dict[str, str]":
    """Map generated column names to their expressions from a stored CREATE TABLE statement.

    Args:
        create_sql: Stored ``CREATE TABLE`` statement.

    Returns:
        Generated column expressions keyed by column name; empty when the statement cannot be parsed.
    """
    table = _parse_create_table(create_sql)
    if not isinstance(table, exp.Create):
        return {}
    expressions: dict[str, str] = {}
    for definition in table.find_all(exp.ColumnDef):
        for constraint in definition.args.get("constraints") or ():
            kind = constraint.args.get("kind")
            if isinstance(kind, exp.ComputedColumnConstraint) and kind.this is not None:
                expressions[definition.name] = kind.this.sql(dialect="sqlite")
    return expressions


def _parse_create_table(create_sql: "str | None") -> "exp.Create | str | None":
    """Parse a stored CREATE TABLE statement, returning the parser error text when it fails."""
    if not create_sql:
        return None
    try:
        parsed = sqlglot.parse_one(create_sql, read="sqlite")
    except SqlglotError as exc:
        return str(exc)
    return parsed if isinstance(parsed, exp.Create) else None


def _check_row(
    schema_name: "str | None",
    table_name: str,
    constraint_name: "str | None",
    check: exp.CheckColumnConstraint,
    column_name: "str | None",
) -> "dict[str, Any]":
    """Build a CHECK constraint metadata row."""
    return {
        "schema_name": schema_name,
        "table_name": table_name,
        "constraint_name": constraint_name,
        "constraint_type": "CHECK",
        "columns": [column_name] if column_name else [],
        "check_clause": check.this.sql(dialect="sqlite"),
    }


def _terminate_statement(sql_text: str) -> str:
    """Return ``sql_text`` with exactly one trailing semicolon."""
    return f"{sql_text.strip().rstrip(';').rstrip()};"
