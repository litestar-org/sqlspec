"""Migration file fix operations for converting timestamp to sequential versions.

This module provides utilities to convert timestamp-format migration files to
sequential format, supporting the hybrid versioning workflow where development
uses timestamps and production uses sequential numbers.
"""

import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlspec.utils.logging import get_logger

__all__ = ("MigrationFixer", "MigrationRename")

logger = get_logger(__name__)


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

        """
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_dir = self.migrations_path / f".backup_{timestamp}"

        backup_dir.mkdir(parents=True, exist_ok=False)

        for file_path in self.migrations_path.iterdir():
            if file_path.is_file() and not file_path.name.startswith("."):
                shutil.copy2(file_path, backup_dir / file_path.name)

        self.backup_path = backup_dir
        return backup_dir

    def apply_renames(self, renames: "list[MigrationRename]", dry_run: bool = False) -> None:
        """Execute planned rename operations.

        Args:
            renames: List of planned rename operations.
            dry_run: If True, log operations without executing.

        """
        if not renames:
            return

        for rename in renames:
            if dry_run:
                continue

            if rename.needs_content_update:
                self.update_file_content(rename.old_path, rename.old_version, rename.new_version)

            rename.old_path.rename(rename.new_path)

    def update_file_content(self, file_path: Path, old_version: "str | None", new_version: "str | None") -> None:
        """Update SQL query names and version comments in file content.

        Transforms query names and version metadata from old version to new version:
            -- name: migrate-{old_version}-up  →  -- name: migrate-{new_version}-up
            -- name: migrate-{old_version}-down  →  -- name: migrate-{new_version}-down
            -- Version: {old_version}  →  -- Version: {new_version}

        Creates version-specific regex patterns to avoid unintended replacements
        of other migrate-* patterns in the file.

        Args:
            file_path: Path to file to update.
            old_version: Old version string (None values skipped gracefully).
            new_version: New version string (None values skipped gracefully).

        """
        if not old_version or not new_version:
            logger.warning("Skipping content update - missing version information")
            return

        content = file_path.read_text(encoding="utf-8")

        up_pattern = re.compile(rf"(-- name:\s+migrate-){re.escape(old_version)}(-up)")
        down_pattern = re.compile(rf"(-- name:\s+migrate-){re.escape(old_version)}(-down)")
        version_pattern = re.compile(rf"(-- Version:\s+){re.escape(old_version)}")

        content = up_pattern.sub(rf"\g<1>{new_version}\g<2>", content)
        content = down_pattern.sub(rf"\g<1>{new_version}\g<2>", content)
        content = version_pattern.sub(rf"\g<1>{new_version}", content)

        file_path.write_text(content, encoding="utf-8")
        logger.debug("Updated content in %s", file_path.name)

    def rollback(self) -> None:
        """Restore migration files from backup.

        Deletes current migration files and restores from backup directory.
        Only restores if backup exists.
        """
        if not self.backup_path or not self.backup_path.exists():
            return

        for file_path in self.migrations_path.iterdir():
            if file_path.is_file() and not file_path.name.startswith("."):
                file_path.unlink()

        for backup_file in self.backup_path.iterdir():
            if backup_file.is_file():
                shutil.copy2(backup_file, self.migrations_path / backup_file.name)

    def cleanup(self) -> None:
        """Remove backup directory after successful conversion.

        Only removes backup if it exists. Logs warning if no backup found.
        """
        if not self.backup_path or not self.backup_path.exists():
            return

        shutil.rmtree(self.backup_path)
        self.backup_path = None
