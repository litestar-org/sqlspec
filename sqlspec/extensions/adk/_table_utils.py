"""Private ADK table helper utilities."""

import re
from collections.abc import Callable
from typing import Final

__all__ = ("ensure_table_name", "owner_id_column_name", "reset_drop_sql", "unique_statements")

_VALID_TABLE_NAME_PATTERN: Final = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_COLUMN_NAME_PATTERN: Final = re.compile(r"^(\w+)")
_MAX_TABLE_NAME_LENGTH: Final = 63


def unique_statements(statements: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for statement in statements:
        if statement in seen:
            continue
        result.append(statement)
        seen.add(statement)
    return result


def owner_id_column_name(owner_id_column_ddl: str) -> str:
    """Extract column name from owner ID column DDL definition."""
    match = _COLUMN_NAME_PATTERN.match(owner_id_column_ddl.strip())
    if not match:
        msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Must start with column name."
        raise ValueError(msg)
    return match.group(1)


def reset_drop_sql(
    current_statements: list[str], table_names: tuple[str, ...], drop_for_table: Callable[[str], list[str]]
) -> list[str]:
    statements = list(current_statements)
    for table_name in table_names:
        statements.extend(drop_for_table(table_name))
    return unique_statements(statements)


def ensure_table_name(table_name: str) -> None:
    """Validate table name for SQL safety."""
    if not table_name:
        msg = "Table name cannot be empty"
        raise ValueError(msg)

    if len(table_name) > _MAX_TABLE_NAME_LENGTH:
        msg = f"Table name too long: {len(table_name)} chars (max {_MAX_TABLE_NAME_LENGTH})"
        raise ValueError(msg)

    if not _VALID_TABLE_NAME_PATTERN.match(table_name):
        msg = (
            f"Invalid table name: {table_name!r}. "
            "Must start with letter/underscore and contain only alphanumeric characters and underscores"
        )
        raise ValueError(msg)
