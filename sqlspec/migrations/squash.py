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

__all__ = ("MigrationSquasher", "SquashPlan", "group_migrations_by_type")

logger = get_logger("sqlspec.migrations.squash")


def group_migrations_by_type(migrations: list[tuple[str, Path]]) -> list[tuple[str, list[tuple[str, Path]]]]:
    """Group consecutive migrations by file type (sql or py).

    Partitions a list of migrations into groups where each group contains
    consecutive migrations of the same type. This enables squashing mixed
    SQL and Python migrations into separate output files.

    Args:
        migrations: List of (version, path) tuples to group.

    Returns:
        List of (type, migrations) tuples where type is "sql" or "py"
        and migrations is the list of (version, path) for that group.

    """
    if not migrations:
        return []

    groups: list[tuple[str, list[tuple[str, Path]]]] = []
    current_type: str | None = None
    current_group: list[tuple[str, Path]] = []

    for version, path in migrations:
        file_type = "py" if path.suffix == ".py" else "sql"

        if file_type != current_type:
            if current_group and current_type is not None:
                groups.append((current_type, current_group))
            current_type = file_type
            current_group = [(version, path)]
        else:
            current_group.append((version, path))

    if current_group and current_type is not None:
        groups.append((current_type, current_group))

    return groups


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

    def plan_squash(
        self,
        start_version: str,
        end_version: str,
        description: str,
        *,
        allow_gaps: bool = False,
        output_format: str = "sql",
    ) -> list[SquashPlan]:
        """Plan a squash operation for a range of migrations.

        For homogeneous migrations (all SQL or all Python), returns a single plan.
        For mixed SQL/Python migrations, returns multiple plans - one per
        consecutive group of same-type migrations.

        Args:
            start_version: First version in the range to squash (inclusive).
            end_version: Last version in the range to squash (inclusive).
            description: Description for the squashed migration file.
            allow_gaps: If True, allow gaps in version sequence.
            output_format: Output file format ("sql" or "py").

        Returns:
            List of SquashPlan objects with details of planned operations.

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

        # Validate no gaps in sequence (unless allow_gaps is True)
        if not allow_gaps and len(source_migrations) > 1:
            source_versions_int = sorted(int(v) for v, _ in source_migrations)
            for i in range(1, len(source_versions_int)):
                if source_versions_int[i] - source_versions_int[i - 1] != 1:
                    msg = f"Gap detected in version sequence between {source_versions_int[i - 1]:04d} and {source_versions_int[i]:04d}"
                    raise SquashValidationError(msg)

        # Group migrations by type (sql vs py) unless output_format forces a specific format
        if output_format == "py":
            # Force all output to Python format - single plan with all migrations
            extension = ".py"
            target_version = f"{int(start_version):04d}"
            target_path = self.migrations_path / f"{target_version}_{description}{extension}"
            return [
                SquashPlan(
                    source_migrations=source_migrations,
                    target_version=target_version,
                    target_path=target_path,
                    description=description,
                    source_versions=[v for v, _ in source_migrations],
                )
            ]

        # Default: group by type and generate appropriate files
        groups = group_migrations_by_type(source_migrations)

        # Build plans for each group
        plans: list[SquashPlan] = []
        version_counter = int(start_version)

        for file_type, group_migrations in groups:
            group_versions = [v for v, _ in group_migrations]
            target_version = f"{version_counter:04d}"
            extension = ".py" if file_type == "py" else ".sql"
            target_path = self.migrations_path / f"{target_version}_{description}{extension}"

            plans.append(
                SquashPlan(
                    source_migrations=group_migrations,
                    target_version=target_version,
                    target_path=target_path,
                    description=description,
                    source_versions=group_versions,
                )
            )
            version_counter += 1

        return plans

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

        lines.extend((
            f"-- {title}",
            f"-- Version: {plan.target_version}",
            f"-- Description: {plan.description}",
            f"-- Squashed from: {', '.join(plan.source_versions)}",
            "",
            f"-- name: migrate-{plan.target_version}-up",
        ))
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

    def generate_python_squash(self, plan: SquashPlan, up_sql: list[str], down_sql: list[str]) -> str:
        """Generate Python migration file content instead of SQL.

        Creates a Python migration file with up() and down() functions
        that return the SQL statements as lists.

        Args:
            plan: The SquashPlan describing the squash operation.
            up_sql: List of UP SQL statements (in execution order).
            down_sql: List of DOWN SQL statements (in rollback order).

        Returns:
            Complete Python file content as a string.
        """
        lines: list[str] = []

        # Module docstring
        title = "SQLSpec Migration"
        if self.template_settings and self.template_settings.profile:
            title = self.template_settings.profile.title

        lines.extend([
            '"""' + title + ".",
            "",
            f"Version: {plan.target_version}",
            f"Description: {plan.description}",
            f"Squashed from: {', '.join(plan.source_versions)}",
            '"""',
            "",
        ])

        # Generate up() function
        lines.extend(["def up() -> list[str]:", '    """Return UP migration SQL statements."""', "    return ["])
        lines.extend(f"        {statement!r}," for statement in up_sql)
        lines.extend(["    ]", ""])

        # Generate down() function
        lines.extend(["def down() -> list[str] | None:", '    """Return DOWN migration SQL statements."""'])
        if down_sql:
            lines.append("    return [")
            lines.extend(f"        {statement!r}," for statement in down_sql)
            lines.extend(["    ]", ""])
        else:
            lines.extend(["    return None", ""])

        return "\n".join(lines)

    def apply_squash(self, plans: list[SquashPlan], *, dry_run: bool = False) -> None:
        """Apply the squash operation for one or more plans.

        Creates backup, writes squashed files, deletes source migrations,
        and cleans up backup on success. Rolls back on error.

        Args:
            plans: List of SquashPlan objects to execute.
            dry_run: If True, no files are modified (preview only).
        """
        if dry_run:
            logger.debug("Dry run mode - no changes will be made")
            return

        # Create backup before making changes
        self._create_backup()

        try:
            for plan in plans:
                # Extract SQL from source migrations
                up_sql, down_sql = self.extract_sql(plan.source_migrations)

                # Generate squashed content based on target file type
                if plan.target_path.suffix == ".py":
                    content = self.generate_python_squash(plan, up_sql, down_sql)
                else:
                    content = self.generate_squashed_content(plan, up_sql, down_sql)

                # Write the squashed file
                plan.target_path.write_text(content, encoding="utf-8")
                logger.debug("Wrote squashed migration to %s", plan.target_path)

            # Collect all source paths to delete (avoid duplicates across plans)
            all_source_paths = {source_path for plan in plans for _, source_path in plan.source_migrations}

            # Delete all source migration files
            for source_path in all_source_paths:
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
