"""Utility functions for SQLSpec migrations.

This module provides helper functions for migration operations.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterProtocol

__all__ = ("create_migration_file", "drop_all", "get_author")


def create_migration_file(migrations_dir: Path, version: str, message: str) -> Path:
    """Create a new migration file from template.

    Args:
        migrations_dir: Directory to create the migration in.
        version: Version number for the migration.
        message: Description message for the migration.

    Returns:
        Path to the created migration file.
    """
    # Sanitize message for filename
    safe_message = message.lower()
    safe_message = "".join(c if c.isalnum() or c in " -" else "" for c in safe_message)
    safe_message = safe_message.replace(" ", "_").replace("-", "_")
    safe_message = "_".join(filter(None, safe_message.split("_")))[:50]

    filename = f"{version}_{safe_message}.sql"
    file_path = migrations_dir / filename

    # Generate template content
    template = f"""-- SQLSpec Migration
-- Version: {version}
-- Description: {message}
-- Created: {datetime.now(timezone.utc).isoformat()}
-- Author: {get_author()}

-- name: migrate-{version}-up
-- TODO: Add your upgrade SQL statements here
-- Example:
-- CREATE TABLE example (
--     id INTEGER PRIMARY KEY,
--     name TEXT NOT NULL
-- );

-- name: migrate-{version}-down
-- TODO: Add your downgrade SQL statements here (optional)
-- Example:
-- DROP TABLE example;
"""

    file_path.write_text(template)
    return file_path


def get_author() -> str:
    """Get current user for migration metadata.

    Returns:
        Username from environment or 'unknown'.
    """
    return os.environ.get("USER", "unknown")


async def drop_all(
    engine: "AsyncDriverAdapterProtocol[Any]", version_table_name: str, metadata: Optional[Any] = None
) -> None:
    """Drop all tables from the database.

    This is a placeholder for database-specific implementations.

    Args:
        engine: The database engine/driver.
        version_table_name: Name of the version tracking table.
        metadata: Optional metadata object.

    Raises:
        NotImplementedError: Always, as this requires database-specific logic.
    """
    # This would need database-specific implementation
    # For now, it's a placeholder
    msg = "drop_all functionality requires database-specific implementation"
    raise NotImplementedError(msg)
