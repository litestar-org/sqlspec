"""Snowflake migration tracking and reversible session schema switches."""

from sqlspec.builder import CreateTable, Select
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.utils.text import normalize_identifier, quote_identifier, split_qualified_identifier

__all__ = ("SnowflakeSyncMigrationTracker",)


class SnowflakeSyncMigrationTracker(SyncMigrationTracker):
    """Track migrations with Snowflake types and explicit lowercase result aliases."""

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
