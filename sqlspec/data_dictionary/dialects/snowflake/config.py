"""Snowflake dialect configuration and shared SHOW KEYS reflection helpers."""

import re
from collections.abc import Mapping, Sequence
from typing import Any, Final, cast

from sqlspec.data_dictionary import DialectConfig, FeatureFlags, FeatureVersions, ForeignKeyMetadata, register_dialect
from sqlspec.utils.text import quote_identifier

__all__ = (
    "SNOWFLAKE_CONFIG",
    "SNOWFLAKE_FEATURE_FLAGS",
    "SNOWFLAKE_FEATURE_VERSIONS",
    "SNOWFLAKE_TYPE_MAPPINGS",
    "SNOWFLAKE_VERSION_PATTERN",
    "parse_snowflake_imported_keys",
    "parse_snowflake_primary_or_unique_keys",
    "qualify_snowflake_information_schema",
)

SNOWFLAKE_VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(r"^(\d+)\.(\d+)(?:\.(\d+))?")
SNOWFLAKE_FEATURE_VERSIONS: Final[FeatureVersions] = {}

SNOWFLAKE_FEATURE_FLAGS: Final[FeatureFlags] = {
    "supports_arrays": True,
    "supports_clustering": True,
    "supports_cte": True,
    "supports_for_update": False,
    "supports_geography": True,
    "supports_json": True,
    "supports_maps": True,
    "supports_merge": True,
    "supports_on_conflict": False,
    "supports_prepared_statements": True,
    "supports_returning": False,
    "supports_roles": True,
    "supports_schemas": True,
    "supports_sequences": True,
    "supports_skip_locked": False,
    "supports_structs": True,
    "supports_time_travel": True,
    "supports_transactional_ddl": False,
    "supports_transactions": True,
    "supports_update_from": True,
    "supports_upsert": True,
    "supports_uuid": False,
    "supports_vectors": True,
    "supports_window_functions": True,
}

SNOWFLAKE_TYPE_MAPPINGS: Final[dict[str, str]] = {
    "array": "ARRAY",
    "bigint": "NUMBER(38,0)",
    "binary": "BINARY",
    "blob": "BINARY",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "datetime": "TIMESTAMP_TZ",
    "decimal": "NUMBER",
    "double": "FLOAT",
    "float": "FLOAT",
    "integer": "NUMBER(38,0)",
    "json": "VARIANT",
    "struct": "OBJECT",
    "geography": "GEOGRAPHY",
    "numeric": "NUMBER",
    "text": "VARCHAR",
    "time": "TIME",
    "timestamp": "TIMESTAMP_TZ",
    "timestamptz": "TIMESTAMP_TZ",
    "uuid": "VARCHAR(36)",
    "varchar": "VARCHAR({length})",
}

SNOWFLAKE_CONFIG: Final[DialectConfig] = DialectConfig(
    name="snowflake",
    parameter_style="qmark",
    version_pattern=SNOWFLAKE_VERSION_PATTERN,
    feature_versions=SNOWFLAKE_FEATURE_VERSIONS,
    feature_flags=SNOWFLAKE_FEATURE_FLAGS,
    type_mappings=SNOWFLAKE_TYPE_MAPPINGS,
)

register_dialect(SNOWFLAKE_CONFIG)


def _key_sequence(row: dict[str, Any]) -> int:
    value = row.get("key_sequence")
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def parse_snowflake_primary_or_unique_keys(
    rows: Sequence[Mapping[str, Any]], *, unique: bool = False, table_name: str | None = None
) -> list[dict[str, Any]]:
    """Group SHOW PRIMARY/UNIQUE KEYS rows in their composite-column order.

    Args:
        rows: Snowflake SHOW rows with case-insensitive column labels.
        unique: Emit UNIQUE constraints instead of PRIMARY KEY constraints.
        table_name: Optional exact catalog table name filter.

    Returns:
        Constraint records containing ordered columns and catalog identity.
    """
    groups: dict[tuple[str | None, str | None, str, str | None], list[dict[str, Any]]] = {}
    for source in rows:
        row = {str(key).lower(): value for key, value in source.items()}
        table = str(row.get("table_name") or "")
        if not table or not row.get("column_name"):
            continue
        if table_name is not None and table != table_name:
            continue
        key = (row.get("database_name"), row.get("schema_name"), table, row.get("constraint_name"))
        groups.setdefault(key, []).append(row)
    constraints: list[dict[str, Any]] = []
    for key, entries in groups.items():
        columns = list(dict.fromkeys(str(row["column_name"]) for row in sorted(entries, key=_key_sequence)))
        constraints.append({
            "database_name": key[0],
            "schema_name": key[1],
            "table_name": key[2],
            "constraint_name": key[3],
            "constraint_type": "UNIQUE" if unique else "PRIMARY KEY",
            "column_names": columns,
            "columns": columns.copy(),
        })
    return constraints


def _referential_action(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value).upper().replace("_", " ")


def parse_snowflake_imported_keys(
    rows: Sequence[Mapping[str, Any]], *, table_name: str | None = None
) -> list[ForeignKeyMetadata]:
    """Parse SHOW IMPORTED KEYS rows without confusing FK and PK names.

    Args:
        rows: Snowflake SHOW rows with case-insensitive column labels.
        table_name: Optional exact catalog table name filter.

    Returns:
        Foreign key columns ordered by constraint identity and key sequence.
    """
    normalized = [{str(key).lower(): value for key, value in source.items()} for source in rows]
    normalized.sort(
        key=lambda row: (
            str(row.get("fk_database_name") or ""),
            str(row.get("fk_schema_name") or ""),
            str(row.get("fk_table_name") or ""),
            str(row.get("fk_name") or ""),
            _key_sequence(row),
        )
    )
    keys: list[ForeignKeyMetadata] = []
    for row in normalized:
        table = str(row.get("fk_table_name") or "")
        if not table or not all(row.get(field) for field in ("fk_column_name", "pk_table_name", "pk_column_name")):
            continue
        if table_name is not None and table != table_name:
            continue
        keys.append(
            ForeignKeyMetadata(
                table_name=table,
                column_name=str(row["fk_column_name"]),
                referenced_table=str(row["pk_table_name"]),
                referenced_column=str(row["pk_column_name"]),
                constraint_name=str(row["fk_name"]) if row.get("fk_name") else None,
                schema=cast("str | None", row.get("fk_schema_name")),
                referenced_schema=cast("str | None", row.get("pk_schema_name")),
                on_delete=_referential_action(row.get("delete_rule")),
                on_update=_referential_action(row.get("update_rule")),
            )
        )
    return keys


def qualify_snowflake_information_schema(sql: str, database_name: str | None) -> str:
    """Prefix ``information_schema`` references with an exact quoted database name.

    Args:
        sql: Catalog query text referencing the session database's ``information_schema``.
        database_name: Exact database name, or ``None`` to keep the session database.

    Returns:
        Query text reading the named database's ``information_schema``.
    """
    if database_name is None:
        return sql
    return sql.replace("information_schema.", f"{quote_identifier(database_name)}.information_schema.")
