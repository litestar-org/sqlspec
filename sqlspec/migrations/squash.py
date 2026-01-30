"""Migration squash engine for combining multiple migrations into a single file.

This module provides utilities to consolidate multiple sequential migrations
into a single "release" migration file, following the Django-style squash workflow.
"""

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from sqlspec.exceptions import SquashValidationError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.migrations.runner import SyncMigrationRunner
    from sqlspec.migrations.templates import MigrationTemplateSettings

__all__ = ("MigrationSquasher", "SquashPlan")

logger = get_logger("sqlspec.migrations.squash")


@dataclass(slots=True)
class SquashPlan:
    """Represents a planned squash operation.

    Attributes:
        source_migrations: List of (version, path) tuples for migrations being squashed.
        target_version: The version string for the squashed migration.
        target_path: Output file path for the squashed migration.
        description: Combined description for the squashed migration.
        source_versions: List of version strings being replaced (for tracking table updates).
    """

    source_migrations: list[tuple[str, Path]]
    target_version: str
    target_path: Path
    description: str
    source_versions: list[str]


class MigrationSquasher:
    """Core squash engine for combining migrations.

    Provides functionality to plan, validate, and execute migration squash operations.
    Combines multiple sequential migrations into a single file with merged UP/DOWN SQL.
    """

    __slots__ = ("backup_path", "migrations_path", "runner", "template_settings")

    def __init__(
        self,
        migrations_path: Path,
        runner: "SyncMigrationRunner",
        template_settings: "MigrationTemplateSettings | None" = None,
    ) -> None:
        """Initialize the migration squasher.

        Args:
            migrations_path: Path to the migrations directory.
            runner: SyncMigrationRunner instance for loading migrations.
            template_settings: Optional template settings for generating squashed file.
        """
        self.migrations_path = migrations_path
        self.runner = runner
        self.template_settings = template_settings
        self.backup_path: Path | None = None

    def plan_squash(self, start_version: str, end_version: str, description: str) -> SquashPlan:
        """Plan a squash operation for a range of migrations.

        Args:
            start_version: First version in the range to squash (inclusive).
            end_version: Last version in the range to squash (inclusive).
            description: Description for the squashed migration file.

        Returns:
            SquashPlan with details of the planned operation.

        Raises:
            SquashValidationError: If validation fails (invalid range, gaps, etc.).
        """
        # Validate range direction
        if int(start_version) > int(end_version):
            msg = f"Invalid range: start version {start_version} is greater than end version {end_version}"
            raise SquashValidationError(msg)

        # Get all migrations from runner
        all_migrations = self.runner.get_migration_files()
        version_map = dict(all_migrations)

        # Validate versions exist
        if start_version not in version_map:
            msg = f"Start version {start_version} not found in migrations"
            raise SquashValidationError(msg)
        if end_version not in version_map:
            msg = f"End version {end_version} not found in migrations"
            raise SquashValidationError(msg)

        # Filter migrations in range
        start_int = int(start_version)
        end_int = int(end_version)
        source_migrations: list[tuple[str, Path]] = []

        for version, path in all_migrations:
            try:
                version_int = int(version)
            except ValueError:
                continue  # Skip non-sequential versions (ext_*, timestamps)

            if start_int <= version_int <= end_int:
                source_migrations.append((version, path))

        # Validate no gaps in sequence
        if len(source_migrations) > 1:
            source_versions_int = sorted(int(v) for v, _ in source_migrations)
            for i in range(1, len(source_versions_int)):
                if source_versions_int[i] - source_versions_int[i - 1] != 1:
                    msg = f"Gap detected in version sequence between {source_versions_int[i - 1]:04d} and {source_versions_int[i]:04d}"
                    raise SquashValidationError(msg)

        # Build plan
        source_versions = [v for v, _ in source_migrations]
        target_path = self.migrations_path / f"{start_version}_{description}.sql"

        return SquashPlan(
            source_migrations=source_migrations,
            target_version=start_version,
            target_path=target_path,
            description=description,
            source_versions=source_versions,
        )

    def extract_sql(self, migrations: list[tuple[str, Path]]) -> tuple[list[str], list[str]]:
        """Extract UP and DOWN SQL statements from migrations.

        UP statements are accumulated in version order.
        DOWN statements are accumulated in REVERSE version order for proper rollback.

        Args:
            migrations: List of (version, path) tuples to extract SQL from.

        Returns:
            Tuple of (up_statements, down_statements) lists.
        """
        up_statements: list[str] = []
        down_statements: list[str] = []

        # Load and collect SQL from each migration (UP in order)
        migration_sql: list[tuple[str, list[str], list[str]]] = []

        for version, path in migrations:
            migration_data = self.runner.load_migration(path, version)
            loader = migration_data["loader"]

            # Get UP SQL
            up_sql = loader.get_up_sql(path)
            if up_sql:
                if isinstance(up_sql, list):
                    up_statements.extend(up_sql)
                else:
                    up_statements.append(up_sql)

            # Get DOWN SQL (collect for reverse ordering)
            down_sql = loader.get_down_sql(path)
            if down_sql:
                if isinstance(down_sql, list):
                    migration_sql.append((version, [], list(down_sql)))
                else:
                    migration_sql.append((version, [], [down_sql]))
            else:
                migration_sql.append((version, [], []))

        # DOWN statements in REVERSE order
        for _, _, down_sql in reversed(migration_sql):
            down_statements.extend(down_sql)

        return up_statements, down_statements

    def generate_squashed_content(self, plan: SquashPlan, up_sql: list[str], down_sql: list[str]) -> str:
        """Generate the content for a squashed migration file.

        Args:
            plan: The SquashPlan describing the squash operation.
            up_sql: List of UP SQL statements (in execution order).
            down_sql: List of DOWN SQL statements (in rollback order).

        Returns:
            Complete SQL file content as a string.
        """
        lines: list[str] = []

        # Header section
        title = "SQLSpec Migration"
        if self.template_settings and self.template_settings.profile:
            title = self.template_settings.profile.title

        lines.append(f"-- {title}")
        lines.append(f"-- Version: {plan.target_version}")
        lines.append(f"-- Description: {plan.description}")
        lines.append(f"-- Squashed from: {', '.join(plan.source_versions)}")
        lines.append("")

        # UP section
        lines.append(f"-- name: migrate-{plan.target_version}-up")
        for statement in up_sql:
            lines.append(statement.rstrip())
            if not statement.rstrip().endswith(";"):
                pass  # Don't add extra semicolons
        lines.append("")

        # DOWN section (only if there are statements)
        if down_sql:
            lines.append(f"-- name: migrate-{plan.target_version}-down")
            lines.extend(statement.rstrip() for statement in down_sql)
            lines.append("")

        return "\n".join(lines)

    def apply_squash(self, plan: SquashPlan, *, dry_run: bool = False) -> None:
        """Apply the squash operation.

        Creates backup, writes squashed file, deletes source migrations,
        and cleans up backup on success. Rolls back on error.

        Args:
            plan: The SquashPlan to execute.
            dry_run: If True, no files are modified (preview only).
        """
        if dry_run:
            logger.debug("Dry run mode - no changes will be made")
            return

        # Create backup before making changes
        self._create_backup()

        try:
            # Extract SQL from source migrations
            up_sql, down_sql = self.extract_sql(plan.source_migrations)

            # Generate squashed content
            content = self.generate_squashed_content(plan, up_sql, down_sql)

            # Write the squashed file
            plan.target_path.write_text(content, encoding="utf-8")
            logger.debug("Wrote squashed migration to %s", plan.target_path)

            # Delete source migration files
            for _, source_path in plan.source_migrations:
                if source_path.exists():
                    source_path.unlink()
                    logger.debug("Deleted source migration %s", source_path)

            # Clean up backup on success
            self._cleanup_backup()

        except Exception:
            # Rollback on error
            self._rollback_backup()
            raise

    def _create_backup(self) -> Path:
        """Create timestamped backup directory with all migration files.

        Returns:
            Path to created backup directory.
        """
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_dir = self.migrations_path / f".backup_{timestamp}"

        backup_dir.mkdir(parents=True, exist_ok=False)

        for file_path in self.migrations_path.iterdir():
            if file_path.is_file() and not file_path.name.startswith("."):
                shutil.copy2(file_path, backup_dir / file_path.name)

        self.backup_path = backup_dir
        logger.debug("Created backup at %s", backup_dir)
        return backup_dir

    def _cleanup_backup(self) -> None:
        """Remove backup directory after successful operation."""
        if not self.backup_path or not self.backup_path.exists():
            return

        shutil.rmtree(self.backup_path)
        logger.debug("Cleaned up backup at %s", self.backup_path)
        self.backup_path = None

    def _rollback_backup(self) -> None:
        """Restore migration files from backup on error."""
        if not self.backup_path or not self.backup_path.exists():
            return

        # Delete any partially created files
        for file_path in self.migrations_path.iterdir():
            if file_path.is_file() and not file_path.name.startswith("."):
                file_path.unlink()

        # Restore from backup
        for backup_file in self.backup_path.iterdir():
            if backup_file.is_file():
                shutil.copy2(backup_file, self.migrations_path / backup_file.name)

        logger.debug("Rolled back from backup at %s", self.backup_path)
