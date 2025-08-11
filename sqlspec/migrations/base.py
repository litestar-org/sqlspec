"""Base classes for SQLSpec migrations.

This module provides abstract base classes for migration components.
"""

import operator
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Generic, Optional, TypeVar

from sqlspec._sql import sql
from sqlspec.builder._ddl import CreateTable
from sqlspec.core.statement import SQL
from sqlspec.loader import SQLFileLoader
from sqlspec.migrations.loaders import get_migration_loader
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import run_

__all__ = ("BaseMigrationCommands", "BaseMigrationRunner", "BaseMigrationTracker")


logger = get_logger("migrations.base")

DriverT = TypeVar("DriverT")
ConfigT = TypeVar("ConfigT")


class BaseMigrationTracker(ABC, Generic[DriverT]):
    """Base class for migration version tracking."""

    def __init__(self, version_table_name: str = "ddl_migrations") -> None:
        """Initialize the migration tracker.

        Args:
            version_table_name: Name of the table to track migrations.
        """
        self.version_table = version_table_name

    def _get_create_table_sql(self) -> SQL:
        """Get SQL for creating the tracking table.

        Returns:
            SQL object for table creation.
        """
        builder = CreateTable(self.version_table)
        if not hasattr(builder, "_columns"):
            builder._columns = []
        if not hasattr(builder, "_constraints"):
            builder._constraints = []
        if not hasattr(builder, "_table_options"):
            builder._table_options = {}

        return (
            builder.if_not_exists()
            .column("version_num", "VARCHAR(32)", primary_key=True)
            .column("description", "TEXT")
            .column("applied_at", "TIMESTAMP", not_null=True, default="CURRENT_TIMESTAMP")
            .column("execution_time_ms", "INTEGER")
            .column("checksum", "VARCHAR(64)")
            .column("applied_by", "VARCHAR(255)")
        ).to_statement()

    def _get_current_version_sql(self) -> SQL:
        """Get SQL for retrieving current version.

        Returns:
            SQL object for version query.
        """

        return (
            sql.select("version_num").from_(self.version_table).order_by("version_num DESC").limit(1)
        ).to_statement()

    def _get_applied_migrations_sql(self) -> SQL:
        """Get SQL for retrieving all applied migrations.

        Returns:
            SQL object for migrations query.
        """

        return (sql.select("*").from_(self.version_table).order_by("version_num")).to_statement()

    def _get_record_migration_sql(
        self, version: str, description: str, execution_time_ms: int, checksum: str, applied_by: str
    ) -> SQL:
        """Get SQL for recording a migration.

        Args:
            version: Version number of the migration.
            description: Description of the migration.
            execution_time_ms: Execution time in milliseconds.
            checksum: MD5 checksum of the migration content.
            applied_by: User who applied the migration.

        Returns:
            SQL object for insert.
        """

        return (
            sql.insert(self.version_table)
            .columns("version_num", "description", "execution_time_ms", "checksum", "applied_by")
            .values(version, description, execution_time_ms, checksum, applied_by)
        ).to_statement()

    def _get_remove_migration_sql(self, version: str) -> SQL:
        """Get SQL for removing a migration record.

        Args:
            version: Version number to remove.

        Returns:
            SQL object for delete.
        """

        return (sql.delete().from_(self.version_table).where(sql.version_num == version)).to_statement()

    @abstractmethod
    def ensure_tracking_table(self, driver: DriverT) -> Any:
        """Create the migration tracking table if it doesn't exist."""
        ...

    @abstractmethod
    def get_current_version(self, driver: DriverT) -> Any:
        """Get the latest applied migration version."""
        ...

    @abstractmethod
    def get_applied_migrations(self, driver: DriverT) -> Any:
        """Get all applied migrations in order."""
        ...

    @abstractmethod
    def record_migration(
        self, driver: DriverT, version: str, description: str, execution_time_ms: int, checksum: str
    ) -> Any:
        """Record a successfully applied migration."""
        ...

    @abstractmethod
    def remove_migration(self, driver: DriverT, version: str) -> Any:
        """Remove a migration record."""
        ...


class BaseMigrationRunner(ABC, Generic[DriverT]):
    """Base class for migration execution."""

    def __init__(self, migrations_path: Path) -> None:
        """Initialize the migration runner.

        Args:
            migrations_path: Path to the directory containing migration files.
        """
        self.migrations_path = migrations_path
        self.loader = SQLFileLoader()
        self.project_root: Optional[Path] = None

    def _extract_version(self, filename: str) -> Optional[str]:
        """Extract version from filename.

        Args:
            filename: The migration filename.

        Returns:
            The extracted version string or None.
        """
        parts = filename.split("_", 1)
        return parts[0].zfill(4) if parts and parts[0].isdigit() else None

    def _calculate_checksum(self, content: str) -> str:
        """Calculate MD5 checksum of migration content.

        Args:
            content: The migration file content.

        Returns:
            MD5 checksum hex string.
        """
        import hashlib

        return hashlib.md5(content.encode()).hexdigest()  # noqa: S324

    def _get_migration_files_sync(self) -> "list[tuple[str, Path]]":
        """Get all migration files sorted by version.

        Returns:
            List of tuples containing (version, file_path).
        """
        if not self.migrations_path.exists():
            return []

        migrations = []
        for pattern in ["*.sql", "*.py"]:
            for file_path in self.migrations_path.glob(pattern):
                if file_path.name.startswith("."):
                    continue
                version = self._extract_version(file_path.name)
                if version:
                    migrations.append((version, file_path))

        return sorted(migrations, key=operator.itemgetter(0))

    def _load_migration_metadata(self, file_path: Path) -> "dict[str, Any]":
        """Load migration metadata from file.

        Args:
            file_path: Path to the migration file.

        Returns:
            Migration metadata dictionary.
        """

        loader = get_migration_loader(file_path, self.migrations_path, self.project_root)
        loader.validate_migration_file(file_path)
        content = file_path.read_text(encoding="utf-8")
        checksum = self._calculate_checksum(content)
        version = self._extract_version(file_path.name)
        description = file_path.stem.split("_", 1)[1] if "_" in file_path.stem else ""

        has_upgrade, has_downgrade = True, False

        if file_path.suffix == ".sql":
            up_query, down_query = f"migrate-{version}-up", f"migrate-{version}-down"
            self.loader.clear_cache()
            self.loader.load_sql(file_path)
            has_upgrade, has_downgrade = self.loader.has_query(up_query), self.loader.has_query(down_query)
        else:
            try:
                has_downgrade = bool(run_(loader.get_down_sql)(file_path))
            except Exception:
                has_downgrade = False

        return {
            "version": version,
            "description": description,
            "file_path": file_path,
            "checksum": checksum,
            "has_upgrade": has_upgrade,
            "has_downgrade": has_downgrade,
            "loader": loader,
        }

    def _get_migration_sql(self, migration: "dict[str, Any]", direction: str) -> Optional[SQL]:
        """Get migration SQL for given direction.

        Args:
            migration: Migration metadata.
            direction: Either 'up' or 'down'.

        Returns:
            SQL object for the migration.
        """
        if not migration.get(f"has_{direction}grade"):
            if direction == "down":
                logger.warning("Migration %s has no downgrade query", migration["version"])
                return None
            msg = f"Migration {migration['version']} has no upgrade query"
            raise ValueError(msg)

        file_path, loader = migration["file_path"], migration["loader"]

        try:
            method = loader.get_up_sql if direction == "up" else loader.get_down_sql
            sql_statements = run_(method)(file_path)

        except Exception as e:
            if direction == "down":
                logger.warning("Failed to load downgrade for migration %s: %s", migration["version"], e)
                return None
            msg = f"Failed to load upgrade for migration {migration['version']}: {e}"
            raise ValueError(msg) from e
        else:
            if sql_statements:
                return SQL(sql_statements[0])
            return None

    @abstractmethod
    def get_migration_files(self) -> Any:
        """Get all migration files sorted by version."""
        ...

    @abstractmethod
    def load_migration(self, file_path: Path) -> Any:
        """Load a migration file and extract its components."""
        ...

    @abstractmethod
    def execute_upgrade(self, driver: DriverT, migration: "dict[str, Any]") -> Any:
        """Execute an upgrade migration."""
        ...

    @abstractmethod
    def execute_downgrade(self, driver: DriverT, migration: "dict[str, Any]") -> Any:
        """Execute a downgrade migration."""
        ...

    @abstractmethod
    def load_all_migrations(self) -> Any:
        """Load all migrations into a single namespace for bulk operations."""
        ...


class BaseMigrationCommands(ABC, Generic[ConfigT, DriverT]):
    """Base class for migration commands."""

    def __init__(self, config: ConfigT) -> None:
        """Initialize migration commands.

        Args:
            config: The SQLSpec configuration.
        """
        self.config = config
        migration_config = getattr(self.config, "migration_config", {}) or {}

        self.version_table = migration_config.get("version_table_name", "sqlspec_migrations")
        self.migrations_path = Path(migration_config.get("script_location", "migrations"))
        self.project_root = Path(migration_config["project_root"]) if "project_root" in migration_config else None

    def _get_init_readme_content(self) -> str:
        """Get README content for migration directory initialization.

        Returns:
            README markdown content.
        """
        return """# SQLSpec Migrations

This directory contains database migration files.

## File Format

Migration files use SQLFileLoader's named query syntax with versioned names:

```sql
-- name: migrate-0001-up
CREATE TABLE example (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- name: migrate-0001-down
DROP TABLE example;
```

## Naming Conventions

### File Names

Format: `{version}_{description}.sql`

- Version: Zero-padded 4-digit number (0001, 0002, etc.)
- Description: Brief description using underscores
- Example: `0001_create_users_table.sql`

### Query Names

- Upgrade: `migrate-{version}-up`
- Downgrade: `migrate-{version}-down`

This naming ensures proper sorting and avoids conflicts when loading multiple files.
"""

    def init_directory(self, directory: str, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in.
            package: Whether to create __init__.py file.
        """
        from rich.console import Console

        console = Console()

        migrations_dir = Path(directory)
        migrations_dir.mkdir(parents=True, exist_ok=True)

        if package:
            (migrations_dir / "__init__.py").touch()

        readme = migrations_dir / "README.md"
        readme.write_text(self._get_init_readme_content())

        (migrations_dir / ".gitkeep").touch()

        console.print(f"[green]Initialized migrations in {directory}[/]")

    @abstractmethod
    def init(self, directory: str, package: bool = True) -> Any:
        """Initialize migration directory structure."""
        ...

    @abstractmethod
    def current(self, verbose: bool = False) -> Any:
        """Show current migration version."""
        ...

    @abstractmethod
    def upgrade(self, revision: str = "head") -> Any:
        """Upgrade to a target revision."""
        ...

    @abstractmethod
    def downgrade(self, revision: str = "-1") -> Any:
        """Downgrade to a target revision."""
        ...

    @abstractmethod
    def stamp(self, revision: str) -> Any:
        """Mark database as being at a specific revision without running migrations."""
        ...

    @abstractmethod
    def revision(self, message: str, file_type: str = "sql") -> Any:
        """Create a new migration file."""
        ...
