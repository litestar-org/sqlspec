"""mssql-python-specific migration tracker."""

import logging
import os
from contextlib import suppress
from typing import TYPE_CHECKING

from sqlspec.builder import CreateTable, sql
from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.migrations.version import parse_version
from sqlspec.observability import resolve_db_system
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.text import split_qualified_identifier

if TYPE_CHECKING:
    from sqlspec.driver import SyncDriverAdapterBase

__all__ = ("MssqlPythonSyncMigrationTracker",)

logger = get_logger("sqlspec.migrations.mssql_python")
_QUALIFIED_IDENTIFIER_MIN_PARTS = 2


class MssqlPythonMigrationTrackerMixin:
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
        schema_name, table_name = _split_schema_table(self.version_table)
        create_sql = self._tracking_table_ddl_text().rstrip().rstrip(";")
        return f"IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '{_escape_sql_literal(table_name)}' AND schema_id = SCHEMA_ID('{_escape_sql_literal(schema_name)}')) BEGIN {create_sql}; END;"

    def _tracking_table_ddl_text(self) -> str:
        """Render CREATE TABLE text without routing SQL Server types through sqlglot."""
        column_lines: list[str] = []
        for column_def in self._tracking_table_ddl().columns:
            default_clause = f" DEFAULT {column_def.default}" if column_def.default else ""
            not_null_clause = " NOT NULL" if column_def.not_null else ""
            primary_key_clause = " PRIMARY KEY" if column_def.primary_key else ""
            column_lines.append(
                f"    {column_def.name} {column_def.dtype}{primary_key_clause}{default_clause}{not_null_clause}"
            )
        return f"CREATE TABLE {self.version_table} (\n" + ",\n".join(column_lines) + "\n)"

    def _existing_columns_query(self) -> str:
        """Return T-SQL query text for migration tracking table columns."""
        schema_name, table_name = _split_schema_table(self.version_table)
        return f"""
            SELECT c.name AS column_name
            FROM sys.columns c
            INNER JOIN sys.tables t ON c.object_id = t.object_id
            WHERE t.name = '{_escape_sql_literal(table_name)}'
              AND t.schema_id = SCHEMA_ID('{_escape_sql_literal(schema_name)}')
        """

    def _add_column_statement_text(self, column_name: str) -> str | None:
        """Return T-SQL ALTER TABLE text for a missing migration column."""
        target_create = self._tracking_table_ddl()
        column_def = next((col for col in target_create.columns if col.name.lower() == column_name), None)
        if column_def is None:
            return None
        default_clause = f" DEFAULT {column_def.default}" if column_def.default else ""
        nullable_clause = " NOT NULL" if column_def.not_null else " NULL"
        return f"ALTER TABLE {self.version_table} ADD {column_def.name} {column_def.dtype}{default_clause}{nullable_clause};"


class MssqlPythonSyncMigrationTracker(MssqlPythonMigrationTrackerMixin, SyncMigrationTracker):
    """T-SQL sync migration tracker."""

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the migration tracking table if it does not exist."""
        driver.execute_script(self._idempotent_tracking_table_ddl_text())
        driver.commit()
        self._migrate_schema_if_needed(driver)

    def record_migration(
        self, driver: "SyncDriverAdapterBase", version: str, description: str, execution_time_ms: int, checksum: str
    ) -> None:
        """Record a successfully applied migration with T-SQL-compatible metadata."""
        parsed_version = parse_version(version)
        version_type = parsed_version.type.value
        result = driver.execute(self._next_execution_sequence_query())
        next_sequence = result.get_data()[0]["next_seq"] if result.data else 1
        driver.execute(
            self._record_migration_statement(
                version,
                version_type,
                next_sequence,
                description,
                execution_time_ms,
                checksum,
                os.environ.get("USER", "unknown"),
            )
        )
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
            driver.commit()
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


def _escape_sql_literal(value: str) -> str:
    """Escape a string for inclusion in a T-SQL string literal."""
    return value.replace("'", "''")


def _split_schema_table(table_name: str) -> tuple[str, str]:
    """Split a schema-qualified table name into schema and table parts."""
    parts = split_qualified_identifier(table_name, quote_chars='"')
    if len(parts) < _QUALIFIED_IDENTIFIER_MIN_PARTS:
        return "dbo", parts[0] if parts else table_name
    schema_name = ".".join(parts[:-1])
    return schema_name or "dbo", parts[-1]
