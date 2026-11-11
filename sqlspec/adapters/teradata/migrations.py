"""Teradata migration tracker with native MULTISET DDL."""

from typing import TYPE_CHECKING

from sqlspec.adapters.teradata.core import TABLE_EXISTS_SQL, format_identifier, split_teradata_table_name
from sqlspec.builder import CreateTable, sql
from sqlspec.migrations.tracker import SyncMigrationTracker

if TYPE_CHECKING:
    from sqlspec.driver import SyncDriverAdapterBase

__all__ = ("TeradataSyncMigrationTracker",)


class TeradataSyncMigrationTracker(SyncMigrationTracker):
    """Track migrations using bounded Teradata metadata columns."""

    def _tracking_table_ddl(self) -> CreateTable:
        return (
            sql
            .create_table(self.version_table)
            .column("version_num", "VARCHAR(32)", primary_key=True)
            .column("version_type", "VARCHAR(16)")
            .column("execution_sequence", "INTEGER")
            .column("description", "VARCHAR(4096)")
            .column("applied_at", "TIMESTAMP(6) WITH TIME ZONE", default="CURRENT_TIMESTAMP(6)", not_null=True)
            .column("execution_time_ms", "INTEGER")
            .column("checksum", "VARCHAR(64)")
            .column("applied_by", "VARCHAR(255)")
            .column("replaces", "VARCHAR(4096)")
        )

    def _tracking_table_exists_sql(self) -> tuple[str, tuple[str | None, str]]:
        """Return the parameterized catalog probe and exact object identity."""
        return TABLE_EXISTS_SQL, split_teradata_table_name(self.version_table)

    def _tracking_table_create_sql(self) -> str:
        """Render native MULTISET DDL without unsupported IF NOT EXISTS."""
        columns = []
        for col in self._tracking_table_ddl().columns:
            default = f" DEFAULT {col.default}" if col.default else ""
            required = " NOT NULL" if col.not_null or col.primary_key else ""
            primary = " PRIMARY KEY" if col.primary_key else ""
            columns.append(f"{format_identifier(col.name)} {col.dtype}{default}{required}{primary}")
        return f"CREATE MULTISET TABLE {format_identifier(self.version_table)} (" + ", ".join(columns) + ")"

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the tracker when missing, then reconcile existing columns."""
        probe, parameters = self._tracking_table_exists_sql()
        if driver.select_one_or_none(probe, *parameters) is None:
            driver.execute_script(self._tracking_table_create_sql())
            self._safe_commit(driver)
        self._migrate_schema_if_needed(driver)
