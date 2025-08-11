"""Utility functions for SQLSpec migrations.

This module provides helper functions for migration operations.
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase

__all__ = ("create_migration_file", "drop_all", "get_author")


def create_migration_file(migrations_dir: Path, version: str, message: str, file_type: str = "sql") -> Path:
    """Create a new migration file from template.

    Args:
        migrations_dir: Directory to create the migration in.
        version: Version number for the migration.
        message: Description message for the migration.
        file_type: Type of migration file to create ('sql' or 'py').

    Returns:
        Path to the created migration file.
    """
    safe_message = message.lower()
    safe_message = "".join(c if c.isalnum() or c in " -" else "" for c in safe_message)
    safe_message = safe_message.replace(" ", "_").replace("-", "_")
    safe_message = "_".join(filter(None, safe_message.split("_")))[:50]

    if file_type == "py":
        filename = f"{version}_{safe_message}.py"
        file_path = migrations_dir / filename
        template = f'''"""SQLSpec Migration - {message}

Version: {version}
Created: {datetime.now(timezone.utc).isoformat()}
Author: {get_author()}

Migration functions can use either naming convention:
- Preferred: up()/down()
- Legacy: migrate_up()/migrate_down()

Both can be synchronous or asynchronous:
- def up(): ...
- async def up(): ...
"""

from typing import List, Union


def up() -> Union[str, List[str]]:
    """Apply the migration (upgrade).

    Returns:
        SQL statement(s) to execute for upgrade.
        Can return a single string or list of strings.

    Note: You can use either 'up()' or 'migrate_up()' for function names.
    Both support async versions: 'async def up()' or 'async def migrate_up()'
    """
    return """
    CREATE TABLE example (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );
    """


def down() -> Union[str, List[str]]:
    """Reverse the migration (downgrade).

    Returns:
        SQL statement(s) to execute for downgrade.
        Can return a single string or list of strings.
        Return empty string or empty list if downgrade is not supported.

    Note: You can use either 'down()' or 'migrate_down()' for function names.
    Both support async versions: 'async def down()' or 'async def migrate_down()'
    """
    return "DROP TABLE example;"
'''
    else:
        filename = f"{version}_{safe_message}.sql"
        file_path = migrations_dir / filename
        template = f"""-- SQLSpec Migration
-- Version: {version}
-- Description: {message}
-- Created: {datetime.now(timezone.utc).isoformat()}
-- Author: {get_author()}

-- name: migrate-{version}-up
CREATE TABLE placeholder (
    id INTEGER PRIMARY KEY
);

-- name: migrate-{version}-down
DROP TABLE placeholder;
"""

    file_path.write_text(template)
    return file_path


def get_author() -> str:
    """Get current user for migration metadata.

    Returns:
        Username from environment or 'unknown'.
    """
    return os.environ.get("USER", "unknown")


async def drop_all(engine: "AsyncDriverAdapterBase", version_table_name: str, metadata: Optional[Any] = None) -> None:
    """Drop all tables from the database.

    Args:
        engine: The database engine/driver.
        version_table_name: Name of the version tracking table.
        metadata: Optional metadata object.

    Raises:
        NotImplementedError: Always raised.
    """
    msg = "drop_all functionality requires database-specific implementation"
    raise NotImplementedError(msg)
