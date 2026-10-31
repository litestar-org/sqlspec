"""SAP SQL Anywhere-specific migration tracker."""

import logging
import os
from contextlib import suppress
from typing import TYPE_CHECKING

from sqlspec.adapters.sqlanywhere.core import (
    escape_sql_literal,
    format_identifier,
    quote_double_identifier,
    split_schema_and_table,
)
from sqlspec.builder import CreateTable, sql
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.migrations.version import parse_version
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context

if TYPE_CHECKING:
    from sqlspec.driver import SyncDriverAdapterBase

__all__ = ("SqlanywhereMigrationTrackerMixin", "SqlanywhereSyncMigrationTracker")

logger = get_logger("sqlspec.migrations.sqlanywhere")


class SqlanywhereMigrationTrackerMixin:
    """SAP SQL Anywhere-specific migration table DDL and schema maintenance."""

    __slots__ = ()

    version_table: str

    def _tracking_table_ddl(self) -> CreateTable:
        """Return SQL Anywhere-compatible migration tracking table DDL."""
        return (
            sql
            .create_table(self.version_table)
            .if_not_exists()
            .column("version_num", "VARCHAR(32)", primary_key=True)
            .column("version_type", "VARCHAR(16)")
            .column("execution_sequence", "INTEGER")
            .column("description", "LONG VARCHAR")
            .column("applied_at", "TIMESTAMP", default="CURRENT UTC TIMESTAMP", not_null=True)
            .column("execution_time_ms", "INTEGER")
            .column("checksum", "VARCHAR(64)")
            .column("applied_by", "VARCHAR(255)")
            .column("replaces", "LONG VARCHAR")
        )

    def _idempotent_tracking_table_ddl_text(self) -> str:
        """Render ``CREATE TABLE IF NOT EXISTS`` DDL with SQL Anywhere native column types."""
        column_lines: list[str] = []
        for column_def in self._tracking_table_ddl().columns:
            default_clause = f" DEFAULT {column_def.default}" if column_def.default else ""
            null_clause = " NOT NULL" if column_def.primary_key or column_def.not_null else " NULL"
            primary_key_clause = " PRIMARY KEY" if column_def.primary_key else ""
            column_lines.append(
                f"    {quote_double_identifier(column_def.name)} {column_def.dtype}{default_clause}{null_clause}{primary_key_clause}"
            )
        return (
            f"CREATE TABLE IF NOT EXISTS {format_identifier(self.version_table)} (\n"
            + ",\n".join(column_lines)
            + "\n);"
        )

    def _existing_columns_query(self) -> str:
        """Return SQL Anywhere catalog query text for migration tracking table columns."""
        schema_name, table_name = split_schema_and_table(self.version_table)
        return f"""
            SELECT c.column_name AS column_name
            FROM SYS.SYSTABCOL c
            INNER JOIN SYS.SYSTAB t ON t.table_id = c.table_id
            INNER JOIN SYS.SYSUSER u ON u.user_id = t.creator
            WHERE t.table_name = '{escape_sql_literal(table_name)}'
              AND u.user_name = '{escape_sql_literal(schema_name)}'
        """

    def _add_column_statement_text(self, column_name: str) -> str | None:
        """Return SQL Anywhere ALTER TABLE text for a missing migration column."""
        target_create = self._tracking_table_ddl()
        column_def = next((col for col in target_create.columns if col.name.lower() == column_name), None)
        if column_def is None:
            return None
        default_clause = f" DEFAULT {column_def.default}" if column_def.default else ""
        null_clause = " NOT NULL" if column_def.not_null else " NULL"
        return (
            f"ALTER TABLE {format_identifier(self.version_table)} "
            f"ADD {quote_double_identifier(column_def.name)} {column_def.dtype}{default_clause}{null_clause}"
        )


class SqlanywhereSyncMigrationTracker(SqlanywhereMigrationTrackerMixin, SyncMigrationTracker):
    """Sync migration tracker using SAP SQL Anywhere DDL."""

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
        """Record a successfully applied migration with SQL Anywhere-compatible metadata."""
        parsed_version = parse_version(version)
        version_type = parsed_version.type.value
        result = driver.execute(self._next_execution_sequence_query())
        next_sequence = result.get_data()[0]["next_seq"] if result.data else 1
        applied_by = os.environ.get("USER", "unknown")
        if replaces:
            statement = self._record_squashed_migration_statement(
                version,
                version_type,
                next_sequence,
                description,
                execution_time_ms,
                checksum,
                applied_by,
                ",".join(replaces),
            )
        else:
            statement = self._record_migration_statement(
                version,
                version_type,
                next_sequence,
                description,
                execution_time_ms,
                checksum,
                applied_by,
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
        """Check and add missing tracking table columns through SQL Anywhere catalog tables."""
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
