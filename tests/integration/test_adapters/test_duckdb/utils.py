"""Utility functions for DuckDB integration tests."""

import uuid
from typing import Optional


def get_unique_table_name(base_name: str = "test_table") -> str:
    """Generate a unique table name for test isolation.

    Args:
        base_name: Base name for the table (default: "test_table")

    Returns:
        Unique table name with UUID suffix
    """
    suffix = uuid.uuid4().hex[:8]
    return f"{base_name}_{suffix}"


def get_test_table_ddl(table_name: Optional[str] = None) -> tuple[str, str]:
    """Get DDL for creating a test table with a unique name.

    Args:
        table_name: Optional specific table name, otherwise generates unique name

    Returns:
        Tuple of (table_name, ddl_statement)
    """
    if table_name is None:
        table_name = get_unique_table_name()

    ddl = f"""
    CREATE TABLE {table_name} (
        id INTEGER PRIMARY KEY,
        name VARCHAR NOT NULL,
        value INTEGER DEFAULT 0
    )
    """

    return table_name, ddl
