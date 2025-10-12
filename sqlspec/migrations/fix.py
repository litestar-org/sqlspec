"""Migration file fix operations for converting timestamp to sequential versions.

This module provides utilities to convert timestamp-format migration files to
sequential format, supporting the hybrid versioning workflow where development
uses timestamps and production uses sequential numbers.
"""

import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

__all__ = ("MigrationFixer", "MigrationRename")

logger = logging.getLogger(__name__)

UP_QUERY_PATTERN: Final[re.Pattern[str]] = re.compile(r"(-- name:\s+migrate-)([^-]+)(-up)")
DOWN_QUERY_PATTERN: Final[re.Pattern[str]] = re.compile(r"(-- name:\s+migrate-)([^-]+)(-down)")


@dataclass
class MigrationRename:
    """Represents a planned migration file rename operation.

    Attributes:
        old_path: Current file path.
        new_path: Target file path after rename.
        old_version: Current version string.
        new_version: Target version string.
        needs_content_update: Whether file content needs updating.
            True for SQL files that contain query names.
    """

    old_path: Path
    new_path: Path
    old_version: str
    new_version: str
    needs_content_update: bool


class MigrationFixer:
    """Handles atomic migration file conversion operations.

    Provides backup/rollback functionality and manages conversion from
    timestamp-based migration files to sequential format.
    """

    def __init__(self, migrations_path: Path) -> None:
        """Initialize migration fixer.

        Args:
            migrations_path: Path to migrations directory.
        """
        self.migrations_path = migrations_path
        self.backup_path: Path | None = None

    def plan_renames(self, conversion_map: dict[str, str]) -> list[MigrationRename]:
        """Plan all file rename operations from conversion map.

        Scans migration directory and builds list of MigrationRename objects
        for all files that need conversion. Validates no target collisions.

        Args:
            conversion_map: Dictionary mapping old versions to new versions.

        Returns:
            List of planned rename operations.

        Raises:
            ValueError: If target file already exists or collision detected.
        """
        if not conversion_map:
            return []

        renames: list[MigrationRename] = []

        for old_version, new_version in conversion_map.items():
            matching_files = list(self.migrations_path.glob(f"{old_version}_*"))

            for old_path in matching_files:
                suffix = old_path.suffix
                description = old_path.stem.replace(f"{old_version}_", "")

                new_filename = f"{new_version}_{description}{suffix}"
                new_path = self.migrations_path / new_filename

                if new_path.exists() and new_path != old_path:
                    msg = f"Target file already exists: {new_path}"
                    raise ValueError(msg)

                needs_content_update = suffix == ".sql"

                renames.append(
                    MigrationRename(
                        old_path=old_path,
                        new_path=new_path,
                        old_version=old_version,
                        new_version=new_version,
                        needs_content_update=needs_content_update,
                    )
                )

        return renames

    def create_backup(self) -> Path:
        """Create timestamped backup directory with all migration files.

        Returns:
            Path to created backup directory.

        Raises:
            OSError: If backup creation fails.
        """
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_dir = self.migrations_path / f".backup_{timestamp}"

        backup_dir.mkdir(parents=True, exist_ok=False)

        for file_path in self.migrations_path.iterdir():
            if file_path.is_file() and not file_path.name.startswith("."):
                shutil.copy2(file_path, backup_dir / file_path.name)

        self.backup_path = backup_dir
        logger.info("Created backup at %s", backup_dir)

        return backup_dir

    def apply_renames(self, renames: "list[MigrationRename]", dry_run: bool = False) -> None:
        """Execute planned rename operations.

        Args:
            renames: List of planned rename operations.
            dry_run: If True, log operations without executing.

        Raises:
            OSError: If file operation fails.
        """
        if not renames:
            logger.info("No renames to apply")
            return

        for rename in renames:
            if dry_run:
                logger.info("Would rename: %s -> %s", rename.old_path.name, rename.new_path.name)
                continue

            if rename.needs_content_update:
                self.update_file_content(rename.old_path, rename.old_version, rename.new_version)

            rename.old_path.rename(rename.new_path)
            logger.info("Renamed: %s -> %s", rename.old_path.name, rename.new_path.name)

    def update_file_content(self, file_path: Path, old_version: str, new_version: str) -> None:
        """Update SQL query names in file content.

        Transforms query names from old version to new version:
            -- name: migrate-{old_version}-up  →  -- name: migrate-{new_version}-up
            -- name: migrate-{old_version}-down  →  -- name: migrate-{new_version}-down

        Args:
            file_path: Path to file to update.
            old_version: Old version string.
            new_version: New version string.

        Raises:
            OSError: If file read/write fails.
        """
        content = file_path.read_text()

        content = UP_QUERY_PATTERN.sub(rf"\g<1>{new_version}\g<3>", content)
        content = DOWN_QUERY_PATTERN.sub(rf"\g<1>{new_version}\g<3>", content)

        file_path.write_text(content)
        logger.debug("Updated content in %s", file_path.name)

    def rollback(self) -> None:
        """Restore migration files from backup.

        Deletes current migration files and restores from backup directory.
        Only restores if backup exists.

        Raises:
            OSError: If file operations fail.
        """
        if not self.backup_path or not self.backup_path.exists():
            logger.warning("No backup found, cannot rollback")
            return

        for file_path in self.migrations_path.iterdir():
            if file_path.is_file() and not file_path.name.startswith("."):
                file_path.unlink()

        for backup_file in self.backup_path.iterdir():
            if backup_file.is_file():
                shutil.copy2(backup_file, self.migrations_path / backup_file.name)

        logger.info("Restored files from backup at %s", self.backup_path)

    def cleanup(self) -> None:
        """Remove backup directory after successful conversion.

        Only removes backup if it exists. Logs warning if no backup found.
        """
        if not self.backup_path or not self.backup_path.exists():
            logger.warning("No backup found, nothing to cleanup")
            return

        shutil.rmtree(self.backup_path)
        logger.info("Removed backup directory %s", self.backup_path)
        self.backup_path = None
