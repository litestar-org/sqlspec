"""BigQuery-specific migration tracker."""

import os
from typing import TYPE_CHECKING, Any

from sqlspec.migrations.tracker import SyncMigrationTracker
from sqlspec.migrations.version import parse_version

if TYPE_CHECKING:
    from sqlspec.driver import SyncDriverAdapterBase

__all__ = ("BigQueryMigrationTracker",)


def _normalize_bigquery_path_part(value: str) -> str:
    """Return a BigQuery path part without identifier quotes."""
    return value.strip().replace("`", "").replace("`.`", ".")


def _quote_bigquery_table_path(table_name: str, table_schema: str | None = None) -> str:
    """Return a backtick-quoted BigQuery table path."""
    clean_table = _normalize_bigquery_path_part(table_name)
    if table_schema:
        clean_schema = _normalize_bigquery_path_part(table_schema)
        return f"`{clean_schema}.{clean_table}`"
    return f"`{clean_table}`"


class BigQueryMigrationTracker(SyncMigrationTracker):
    """BigQuery migration tracker using BigQuery table paths and native types."""

    def _qualify_version_table(self, version_table_name: str, version_table_schema: str | None) -> str:
        """Return a BigQuery tracker table reference."""
        return _quote_bigquery_table_path(version_table_name, version_table_schema)

    def _migrate_schema_if_needed(self, driver: "SyncDriverAdapterBase") -> None:
        """Skip generic schema migration checks for BigQuery tracker tables."""
        del driver

    def ensure_tracking_table(self, driver: "SyncDriverAdapterBase") -> None:
        """Create the BigQuery migration tracking table if needed."""
        driver.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.version_table} (
                version_num STRING NOT NULL,
                version_type STRING,
                execution_sequence INT64,
                description STRING,
                applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                execution_time_ms INT64,
                checksum STRING,
                applied_by STRING,
                replaces STRING
            )
        """)
        self._safe_commit(driver)

    def get_current_version(self, driver: "SyncDriverAdapterBase") -> "str | None":
        """Return the latest applied migration version."""
        result = driver.execute(f"""
            SELECT version_num
            FROM {self.version_table}
            ORDER BY execution_sequence DESC
            LIMIT 1
        """)
        return result.get_data()[0]["version_num"] if result.data else None

    def get_applied_migrations(self, driver: "SyncDriverAdapterBase") -> "list[dict[str, Any]]":
        """Return all applied migrations in execution order."""
        result = driver.execute(f"""
            SELECT
                version_num,
                version_type,
                execution_sequence,
                description,
                applied_at,
                execution_time_ms,
                checksum,
                applied_by,
                replaces
            FROM {self.version_table}
            ORDER BY execution_sequence
        """)
        return result.get_data()

    def _next_execution_sequence(self, driver: "SyncDriverAdapterBase") -> int:
        result = driver.execute(f"""
            SELECT COALESCE(MAX(execution_sequence), 0) + 1 AS next_seq
            FROM {self.version_table}
        """)
        return int(result.get_data()[0]["next_seq"]) if result.data else 1

    def record_migration(
        self, driver: "SyncDriverAdapterBase", version: str, description: str, execution_time_ms: int, checksum: str
    ) -> None:
        """Record a successfully applied migration."""
        parsed_version = parse_version(version)
        driver.execute(
            f"""
            INSERT INTO {self.version_table} (
                version_num,
                version_type,
                execution_sequence,
                description,
                execution_time_ms,
                checksum,
                applied_by
            )
            VALUES (
                @version_num,
                @version_type,
                @execution_sequence,
                @description,
                @execution_time_ms,
                @checksum,
                @applied_by
            )
            """,
            {
                "version_num": version,
                "version_type": parsed_version.type.value,
                "execution_sequence": self._next_execution_sequence(driver),
                "description": description,
                "execution_time_ms": execution_time_ms,
                "checksum": checksum,
                "applied_by": os.environ.get("USER", "unknown"),
            },
        )
        self._safe_commit(driver)

    def remove_migration(self, driver: "SyncDriverAdapterBase", version: str) -> None:
        """Remove a migration record."""
        driver.execute(f"DELETE FROM {self.version_table} WHERE version_num = @version_num", {"version_num": version})
        self._safe_commit(driver)

    def update_version_record(self, driver: "SyncDriverAdapterBase", old_version: str, new_version: str) -> None:
        """Update a migration version record."""
        parsed_new_version = parse_version(new_version)
        result = driver.execute(
            f"""
            UPDATE {self.version_table}
            SET version_num = @new_version,
                version_type = @new_version_type
            WHERE version_num = @old_version
            """,
            {"old_version": old_version, "new_version": new_version, "new_version_type": parsed_new_version.type.value},
        )
        if result.rows_affected == 0:
            applied_versions = {row["version_num"] for row in self.get_applied_migrations(driver)}
            if new_version not in applied_versions:
                msg = f"Migration version {old_version} not found in database"
                raise ValueError(msg)
        self._safe_commit(driver)

    def replace_with_squash(
        self,
        driver: "SyncDriverAdapterBase",
        squashed_version: str,
        replaced_versions: "list[str]",
        description: str,
        checksum: str,
    ) -> None:
        """Replace multiple migration records with one squashed migration record."""
        driver.execute(
            f"DELETE FROM {self.version_table} WHERE version_num IN UNNEST(@versions)", {"versions": replaced_versions}
        )
        parsed_version = parse_version(squashed_version)
        driver.execute(
            f"""
            INSERT INTO {self.version_table} (
                version_num,
                version_type,
                execution_sequence,
                description,
                execution_time_ms,
                checksum,
                applied_by,
                replaces
            )
            VALUES (
                @version_num,
                @version_type,
                @execution_sequence,
                @description,
                @execution_time_ms,
                @checksum,
                @applied_by,
                @replaces
            )
            """,
            {
                "version_num": squashed_version,
                "version_type": parsed_version.type.value,
                "execution_sequence": self._next_execution_sequence(driver),
                "description": description,
                "execution_time_ms": 0,
                "checksum": checksum,
                "applied_by": os.environ.get("USER", "unknown"),
                "replaces": ",".join(replaced_versions),
            },
        )
        self._safe_commit(driver)
