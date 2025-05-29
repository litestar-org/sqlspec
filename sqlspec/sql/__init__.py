"""SQL utilities, validation, and parameter handling."""

from typing import Optional, Union

from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import Expression

from sqlspec.sql import builder, filters, parameters, preprocessors, result, statement
from sqlspec.sql.statement import (
    SQLStatement,
    Statement,
    StatementConfig,
)


def select(*columns: Union[str, Expression], dialect: Optional[DialectType] = None) -> builder.SelectBuilder:
    """Create a SELECT builder.

    Args:
        *columns: Optional columns to select. If not provided, selects all columns.
        dialect: Optional SQL dialect to use for the query.

    Returns:
        SelectBuilder: A new SelectBuilder instance with the specified columns.
    """
    if columns:
        builder.SelectBuilder(dialect=dialect).select(*columns)
    return builder.SelectBuilder(dialect=dialect)


def insert(dialect: Optional[DialectType] = None) -> builder.InsertBuilder:
    """Create an INSERT builder.

    Args:
        dialect: Optional SQL dialect to use for the query.

    Returns:
        InsertBuilder: A new InsertBuilder instance.
    """
    return builder.InsertBuilder(dialect=dialect)


def update(table: Optional[str] = None, dialect: Optional[DialectType] = None) -> builder.UpdateBuilder:
    """Create an UPDATE builder.

    Args:
        table: Optional table name to update. If provided, sets the target table for the UPDATE.
        dialect: Optional SQL dialect to use for the query.

    Returns:
        UpdateBuilder: A new UpdateBuilder instance, optionally with the target table set.
    """
    if table:
        return builder.UpdateBuilder(dialect=dialect).table(table)
    return builder.UpdateBuilder(dialect=dialect)


def delete(dialect: Optional[DialectType] = None) -> builder.DeleteBuilder:
    """Create a DELETE builder.

    Args:
        dialect: Optional SQL dialect to use for the query.

    Returns:
        DeleteBuilder: A new DeleteBuilder instance.

    """
    return builder.DeleteBuilder(dialect=dialect)


def merge(dialect: Optional[DialectType] = None) -> builder.MergeBuilder:
    """Create a new MERGE query builder.

    Args:
        dialect: Optional SQL dialect to use for the query.

    Returns:
        MergeBuilder: A new MergeBuilder instance.
    """
    return builder.MergeBuilder(dialect=dialect)


__all__ = (
    "SQLStatement",
    "Statement",
    "StatementConfig",
    "builder",
    "delete",
    "filters",
    "insert",
    "merge",
    "parameters",
    "preprocessors",
    "result",
    "select",
    "statement",
    "update",
)
