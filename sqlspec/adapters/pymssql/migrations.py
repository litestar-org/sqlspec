"""pymssql-specific migration tracker."""

import logging
from contextlib import suppress
from typing import TYPE_CHECKING

from sqlspec.adapters.pymssql.core import (
    escape_sql_literal,
    format_identifier,
    quote_bracket_identifier,
    split_schema_and_table,
)
from sqlspec.builder import CreateTable, sql
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.driver import SyncDriverAdapterBase

__all__ = ("PymssqlSyncMigrationTracker",)

logger = get_logger("sqlspec.migrations.pymssql")


class PymssqlMigrationTrackerMixin:
    """T-SQL-specific migration table DDL and schema maintenance."""

    __slots__ = ()

    version_table: str

    def _tracking_table_ddl(self) -> CreateTable:
        """Return T-SQL-compatible migration tracking table DDL."""
        return (
            sql
            .create_table(self.version_table)
            .column("version_num", "NVARCHAR(32)", primary_key=True)
            .column("version_type", "NVARCHAR(16)")
            .column("execution_sequence", "INT")
            .column("description", "NVARCHAR(MAX)")
            .column("applied_at", "DATETIME2(6)", default="SYSUTCDATETIME()", not_null=True)
            .column("execution_time_ms", "INT")
            .column("checksum", "NVARCHAR(64)")
            .column("applied_by", "NVARCHAR(255)")
            .column("replaces", "NVARCHAR(MAX)")
        )

    def _idempotent_tracking_table_ddl_text(self) -> str:
        """Wrap CREATE TABLE in a T-SQL sys.tables existence probe."""
        schema_name, table_name = split_schema_and_table(self.version_table)
        create_sql = self._tracking_table_ddl_text().rstrip().rstrip(";")
        return f"IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '{escape_sql_literal(table_name)}' AND schema_id = SCHEMA_ID('{escape_sql_literal(schema_name)}')) BEGIN {create_sql}; END;"

    def _tracking_table_ddl_text(self) -> str:
        """Render CREATE TABLE text without routing SQL Server types through sqlglot."""
        column_lines: list[str] = []
        for column_def in self._tracking_table_ddl().columns:
            default_clause = f" DEFAULT {column_def.default}" if column_def.default else ""
            not_null_clause = " NOT NULL" if column_def.not_null else ""
            primary_key_clause = " PRIMARY KEY" if column_def.primary_key else ""
            column_lines.append(
                f"    {quote_bracket_identifier(column_def.name)} {column_def.dtype}{primary_key_clause}{default_clause}{not_null_clause}"
            )
        return f"CREATE TABLE {format_identifier(self.version_table)} (\n" + ",\n".join(column_lines) + "\n)"

    def _existing_columns_query(self) -> str:
        """Return T-SQL query text for migration tracking table columns."""
        schema_name, table_name = split_schema_and_table(self.version_table)
        return f"""
            SELECT c.name AS column_name
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            WHERE t.name = '{escape_sql_literal(table_name)}'
              AND t.schema_id = SCHEMA_ID('{escape_sql_literal(schema_name)}')
        """

    def _add_column_statement_text(self, column_name: str) -> str | None:
        """Return T-SQL ALTER TABLE text for a missing migration column."""
        target_create = self._tracking_table_ddl()
        column_def = next((col for col in target_create.columns if col.name.lower() == column_name), None)
        if column_def is None:
            return None
        default_clause = f" DEFAULT {column_def.default}" if column_def.default else ""
        nullable_clause = " NOT NULL" if (column_def.not_null and column_def.default is not None) else " NULL"
        return f"ALTER TABLE {format_identifier(self.version_table)} ADD {quote_bracket_identifier(column_def.name)} {column_def.dtype}{default_clause}{nullable_clause};"


class PymssqlSyncMigrationTracker(PymssqlMigrationTrackerMixin, SyncMigrationTracker):
    """T-SQL sync migration tracker."""

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it does not exist."""
        driver.execute_script(self._idempotent_tracking_table_ddl_text())
        self._commit_if_needed(driver)
        self._migrate_schema_if_needed(driver)

    def record_migration(
        self,
        driver: "SyncDriverAdapterBase",
        version: str,
        description: str,
        execution_time_ms: int,
        checksum: str,
        *,
        commit: bool = True,
        replaces: "Sequence[str] | None" = None,
    ) -> None:
        """Record a successfully applied migration with T-SQL-compatible metadata.

        Squashed migrations persist their replaced versions in the ``replaces`` column so
        multi-environment reconciliation can recognize fully applied pre-squash histories.
        """
        version_type = self._derive_version_type(version)
        result = driver.execute(self._next_execution_sequence_query())
        next_sequence = self._extract_next_sequence(result)
        statement = self._record_statement(
            version, version_type, next_sequence, description, execution_time_ms, checksum, self._applied_by(), replaces
        )
        driver.execute(statement)
        if commit:
            self._commit_if_needed(driver)

    def _commit_if_needed(self, driver: "SyncDriverAdapterBase") -> None:
        if hasattr(driver, "driver_features"):
            self._safe_commit(driver)
            return
        driver.commit()

    def _migrate_schema_if_needed(self, driver: "SyncDriverAdapterBase") -> None:
        """Check and add missing tracking table columns through SQL Server catalog views."""
        try:
            rows = driver.select(self._existing_columns_query())
            existing_columns = {str(row["column_name"]).lower() for row in rows if row.get("column_name") is not None}
            missing_columns = self._detect_missing_columns(existing_columns)
            if not missing_columns:
                return
            for column_name in sorted(missing_columns):
                self._add_column(driver, column_name)
            self._commit_if_needed(driver)
        except Exception as exc:
            with suppress(Exception):
                driver.rollback()
            log_with_context(
                logger,
                logging.ERROR,
                "migration.track",
                db_system=resolve_db_system(type(driver).__name__),
                table=self.version_table,
                operation="schema_check",
                status="failed",
                error_type=type(exc).__name__,
            )

    def _add_column(self, driver: "SyncDriverAdapterBase", column_name: str) -> None:
        """Add a single missing migration tracking column."""
        add_column_sql = self._add_column_statement_text(column_name)
        if add_column_sql is None:
            return
        driver.execute_script(add_column_sql)
