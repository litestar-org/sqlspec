"""Private ADK table helper utilities."""

import re
from collections.abc import Callable
from typing import Final

__all__ = (
    "ensure_table_name",
    "owner_id_column_name",
    "reset_drop_sql",
    "unique_statements",
    "validate_owner_id_column_ddl",
)

_VALID_TABLE_NAME_PATTERN: Final = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_VALID_COLUMN_NAME_PATTERN: Final = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_TABLE_NAME_LENGTH: Final = 63
_DISALLOWED_COLUMN_DDL_TOKENS: Final = ("\x00", ";", "--", "/*", "*/", "'", '"', "`")


def unique_statements(statements: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for statement in statements:
        if statement in seen:
            continue
        result.append(statement)
        seen.add(statement)
    return result


def validate_owner_id_column_ddl(owner_id_column_ddl: str) -> str:
    """Validate that ``owner_id_column_ddl`` defines a single unquoted column.

    Args:
        owner_id_column_ddl: Raw column DDL fragment from ``extension_config["adk"]``.

    Returns:
        Stripped column DDL definition.

    Raises:
        ValueError: If the definition is empty, contains statement/comment/quote
            tokens, has unbalanced parentheses or top-level commas, or does not
            begin with a valid column identifier followed by a type definition.
    """
    stripped = owner_id_column_ddl.strip()
    if not stripped:
        msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Must start with column name and type."
        raise ValueError(msg)

    for token in _DISALLOWED_COLUMN_DDL_TOKENS:
        if token in stripped:
            msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Contains disallowed token {token!r}."
            raise ValueError(msg)

    depth = 0
    for char in stripped:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Unbalanced parentheses."
                raise ValueError(msg)
        elif char == "," and depth == 0:
            msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Must define a single column."
            raise ValueError(msg)

    if depth != 0:
        msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Unbalanced parentheses."
        raise ValueError(msg)

    parts = stripped.split(None, 1)
    if len(parts) == 1 or not parts[1]:
        msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Must include a column type definition."
        raise ValueError(msg)

    column_name = parts[0]
    if len(column_name) > _MAX_TABLE_NAME_LENGTH or not _VALID_COLUMN_NAME_PATTERN.fullmatch(column_name):
        msg = f"Invalid owner_id_column DDL: {owner_id_column_ddl!r}. Must start with a valid column name."
        raise ValueError(msg)

    return stripped


def owner_id_column_name(owner_id_column_ddl: str) -> str:
    """Extract column name from owner ID column DDL definition."""
    validated = validate_owner_id_column_ddl(owner_id_column_ddl)
    return validated.split(None, 1)[0]


def reset_drop_sql(
    current_statements: list[str], table_names: tuple[str, ...], drop_for_table: Callable[[str], list[str]]
) -> list[str]:
    statements = list(current_statements)
    for table_name in table_names:
        statements.extend(drop_for_table(table_name))
    return unique_statements(statements)


def ensure_table_name(table_name: str, *, max_parts: int = 1) -> None:
    """Validate table identifier components supported by the store.

    Args:
        table_name: Table name to validate.
        max_parts: Maximum number of identifier components the store supports.

    Raises:
        ValueError: If a name component is empty, invalid, or too long.
    """
    if not table_name:
        msg = "Table name cannot be empty"
        raise ValueError(msg)

    parts = table_name.split(".")
    for part in parts:
        if len(part) > _MAX_TABLE_NAME_LENGTH:
            msg = f"Table name too long: {len(part)} chars (max {_MAX_TABLE_NAME_LENGTH})"
            raise ValueError(msg)

    if len(parts) > max_parts or any(not _VALID_TABLE_NAME_PATTERN.fullmatch(part) for part in parts):
        msg = (
            f"Invalid table name: {table_name!r}. "
            "Must start with letter/underscore and contain only alphanumeric characters and underscores"
        )
        raise ValueError(msg)
