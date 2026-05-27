"""Shared PostgreSQL migration schema integration helpers."""

from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from sqlspec.utils.text import quote_identifier

ParamStyle = Literal["numeric", "pyformat"]


def unique_identifier(prefix: str) -> str:
    """Return a short PostgreSQL-safe identifier for integration tests."""
    return f"{prefix}_{uuid4().hex[:10]}"


def write_unqualified_table_migration(migration_dir: Path, table_name: str) -> None:
    """Write a Python migration that creates an unqualified table."""
    migration_content = f'''"""Create an unqualified table."""


def up():
    """Create an unqualified table."""
    return ["""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
    """]


def down():
    """Drop the unqualified table."""
    return ["DROP TABLE IF EXISTS {table_name}"]
'''
    (migration_dir / "0001_create_unqualified_table.py").write_text(migration_content)


def write_non_transactional_unqualified_table_migration(migration_dir: Path, table_name: str) -> None:
    """Write a SQL migration that creates an unqualified table without a transaction."""
    migration_content = f"""
-- transactional: false
-- name: migrate-0001-up
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- name: migrate-0001-down
DROP TABLE IF EXISTS {table_name};
""".strip()
    (migration_dir / "0001_create_unqualified_table.sql").write_text(migration_content)


def create_schema_sql(schema: str) -> str:
    """Return PostgreSQL CREATE SCHEMA SQL for a trusted test identifier."""
    return f"CREATE SCHEMA {quote_identifier(schema)}"


def drop_schema_sql(schema: str) -> str:
    """Return PostgreSQL DROP SCHEMA SQL for a trusted test identifier."""
    return f"DROP SCHEMA IF EXISTS {quote_identifier(schema)} CASCADE"


def table_exists_sql(style: ParamStyle) -> str:
    """Return an information_schema table existence query for the adapter parameter style."""
    if style == "pyformat":
        return "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s"
    return "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2"


def sync_table_exists(driver: Any, schema: str, table_name: str, *, style: ParamStyle) -> bool:
    """Return whether the table exists using a sync SQLSpec driver."""
    result = driver.execute(table_exists_sql(style), (schema, table_name))
    return bool(result.data)


async def async_table_exists(driver: Any, schema: str, table_name: str, *, style: ParamStyle) -> bool:
    """Return whether the table exists using an async SQLSpec driver."""
    result = await driver.execute(table_exists_sql(style), (schema, table_name))
    return bool(result.data)
