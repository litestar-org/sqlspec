"""IBM Db2 migration tracker."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlspec.adapters.db2.core import TABLE_EXISTS_SQL, split_db2_table_name
from sqlspec.builder import sql
from sqlspec.migrations.tracker import SyncMigrationTracker

if TYPE_CHECKING:
    from sqlspec.builder import CreateTable, Insert
    from sqlspec.driver import SyncDriverAdapterBase

__all__ = ("Db2MigrationTrackerMixin", "Db2SyncMigrationTracker")


def _utc_now() -> datetime:
    """Return the current UTC time as a naive ``datetime``."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Db2MigrationTrackerMixin:
    """Db2 tracking-table DDL, catalog existence probe, and UTC audit timestamps."""

    __slots__ = ()

    version_table: str
    version_table_name: str
    version_table_schema: "str | None"

    def _tracking_table_ddl(self) -> "CreateTable":
        """Return Db2 DDL for the migration tracking table.

        Returns:
            CREATE TABLE builder without an existence clause.
        """
        builder = sql.create_table(self.version_table_name)
        if self.version_table_schema:
            builder.in_schema(self.version_table_schema)
        return (
            builder
            .column("version_num", "VARCHAR(32)", primary_key=True, not_null=True)
            .column("version_type", "VARCHAR(16)")
            .column("execution_sequence", "INTEGER")
            .column("description", "CLOB")
            .column("applied_at", "TIMESTAMP", default="CURRENT_TIMESTAMP", not_null=True)
            .column("execution_time_ms", "INTEGER")
            .column("checksum", "VARCHAR(64)")
            .column("applied_by", "VARCHAR(255)")
            .column("replaces", "CLOB")
        )

    def _tracking_table_exists_sql(self) -> "tuple[str, tuple[str | None, str]]":
        """Return the catalog probe for the tracking table and its bound names.

        Returns:
            The probe SQL and its ``(schema, table)`` parameters; the schema is ``None`` for an
            unqualified table so the probe checks ``CURRENT SCHEMA``.
        """
        return TABLE_EXISTS_SQL, split_db2_table_name(self.version_table)

    def _record_migration_statement(
        self,
        version: str,
        version_type: str,
        execution_sequence: int,
        description: str,
        execution_time_ms: int,
        checksum: str,
        applied_by: str,
    ) -> "Insert":
        """Return an INSERT for a migration record with a naive-UTC ``applied_at``.

        Args:
            version: Version number of the migration.
            version_type: Version format type.
            execution_sequence: Application order.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: Checksum of the migration content.
            applied_by: User who applied the migration.

        Returns:
            INSERT builder.
        """
        return (
            sql
            .insert(self.version_table)
            .columns(
                "version_num",
                "version_type",
                "execution_sequence",
                "description",
                "applied_at",
                "execution_time_ms",
                "checksum",
                "applied_by",
            )
            .values(
                version,
                version_type,
                execution_sequence,
                description,
                _utc_now(),
                execution_time_ms,
                checksum,
                applied_by,
            )
        )

    def _record_squashed_migration_statement(
        self,
        version: str,
        version_type: str,
        execution_sequence: int,
        description: str,
        execution_time_ms: int,
        checksum: str,
        applied_by: str,
        replaces: str,
    ) -> "Insert":
        """Return an INSERT for a squashed migration record with a naive-UTC ``applied_at``.

        Args:
            version: Version number of the squashed migration.
            version_type: Version format type.
            execution_sequence: Application order.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: Checksum of the migration content.
            applied_by: User who applied the migration.
            replaces: Comma-separated list of replaced versions.

        Returns:
            INSERT builder.
        """
        return (
            sql
            .insert(self.version_table)
            .columns(
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
            .values(
                version,
                version_type,
                execution_sequence,
                description,
                _utc_now(),
                execution_time_ms,
                checksum,
                applied_by,
                replaces,
            )
        )


class Db2SyncMigrationTracker(Db2MigrationTrackerMixin, SyncMigrationTracker):
    """Db2 synchronous migration tracker."""

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the tracking table when the catalog does not list it, then add missing columns.

        Args:
            driver: The database driver to use.
        """
        probe_sql, parameters = self._tracking_table_exists_sql()
        if driver.select_value_or_none(probe_sql, parameters) is None:
            driver.execute(self._tracking_table_ddl())
            self._safe_commit(driver)
        self._migrate_schema_if_needed(driver)
