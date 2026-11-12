"""Snowflake migration tracking and reversible session schema switches."""

from typing import TYPE_CHECKING

from sqlspec.builder import CreateTable, Select
from sqlspec.exceptions import SQLSpecError
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.utils.text import normalize_identifier, quote_identifier, split_qualified_identifier

if TYPE_CHECKING:
    from sqlspec.driver import SyncDriverAdapterBase

__all__ = ("SnowflakeSyncMigrationTracker", "apply_snowflake_migration_schema", "restore_snowflake_migration_schema")

_PREVIOUS_SCHEMA_KEY = "_migration_previous_schema"


def apply_snowflake_migration_schema(driver: "SyncDriverAdapterBase", schema: str) -> None:
    """Capture the exact database/schema and switch to a validated schema name."""
    from sqlspec.adapters.snowflake.driver import _format_identifier

    quoted = _format_identifier(schema)
    if driver.driver_features.get(_PREVIOUS_SCHEMA_KEY) is None:
        current = driver.select_one('SELECT CURRENT_DATABASE() AS "database_name", CURRENT_SCHEMA() AS "schema_name"')
        database_name, schema_name = current.get("database_name"), current.get("schema_name")
        if not isinstance(schema_name, str) or not schema_name:
            msg = "Cannot switch Snowflake migration schema without a restorable current schema"
            raise SQLSpecError(msg)
        previous = quote_identifier(schema_name)
        if isinstance(database_name, str) and database_name:
            previous = f"{quote_identifier(database_name)}.{previous}"
        driver.driver_features[_PREVIOUS_SCHEMA_KEY] = previous
    driver.execute(f"USE SCHEMA {quoted}")


def restore_snowflake_migration_schema(driver: "SyncDriverAdapterBase") -> None:
    """Restore captured exact-case identifiers, clearing state only after success."""
    previous = driver.driver_features.get(_PREVIOUS_SCHEMA_KEY)
    if previous is None:
        return
    if not isinstance(previous, str) or not previous:
        msg = "Invalid captured Snowflake migration schema"
        raise SQLSpecError(msg)
    driver.execute(f"USE SCHEMA {previous}")
    driver.driver_features.pop(_PREVIOUS_SCHEMA_KEY, None)


class SnowflakeSyncMigrationTracker(SyncMigrationTracker):
    """Track migrations with Snowflake types and explicit lowercase result aliases."""

    __slots__ = ()

    def _qualify_version_table(self, version_table_name: str, version_table_schema: str | None) -> str:
        name = quote_identifier(normalize_identifier(version_table_name, "snowflake"))
        return (
            f"{quote_identifier(normalize_identifier(version_table_schema, 'snowflake'))}.{name}"
            if version_table_schema
            else name
        )

    def catalog_schema_name(self) -> str | None:
        """Keep exact schema names quoted for catalog introspection."""
        parts = split_qualified_identifier(self.version_table, quote_chars='"', allow_bracket_quotes=False)
        return quote_identifier(parts[-2]) if len(parts) > 1 else None

    def _tracking_table_ddl(self) -> CreateTable:
        return (
            self
            ._tracking_table_builder()
            .if_not_exists()
            .column("version_num", "VARCHAR(32)", primary_key=True)
            .column("version_type", "VARCHAR(16)")
            .column("execution_sequence", "INTEGER")
            .column("description", "VARCHAR")
            .column("applied_at", "TIMESTAMP_TZ", default="CURRENT_TIMESTAMP()", not_null=True)
            .column("execution_time_ms", "INTEGER")
            .column("checksum", "VARCHAR(64)")
            .column("applied_by", "VARCHAR(255)")
            .column("replaces", "VARCHAR")
        )

    def _current_version_query(self) -> Select:
        return (
            Select('version_num AS "version_num"')
            .from_(self.version_table)
            .order_by("execution_sequence DESC")
            .limit(1)
        )

    def _applied_migrations_query(self) -> Select:
        columns = (
            "version_num",
            "version_type",
            "execution_sequence",
            "description",
            "applied_at",
            "execution_time_ms",
            "checksum",
            "applied_by",
            "replaces",
        )
        return (
            Select(*(f'{name} AS "{name}"' for name in columns))
            .from_(self.version_table)
            .order_by("execution_sequence")
        )

    def _next_execution_sequence_query(self) -> Select:
        return Select('COALESCE(MAX(execution_sequence), 0) + 1 AS "next_seq"').from_(self.version_table)

    def _check_versions_query(self, versions: list[str]) -> Select:
        return Select('version_num AS "version_num"').from_(self.version_table).where_in("version_num", versions)
