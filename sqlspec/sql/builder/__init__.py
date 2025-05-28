"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

from typing import Optional, Union

from sqlglot import exp
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLBuilderError
from sqlspec.sql.builder._base import QueryBuilder, SafeQuery
from sqlspec.sql.builder._delete import DeleteBuilder
from sqlspec.sql.builder._insert import InsertBuilder
from sqlspec.sql.builder._merge import MergeBuilder
from sqlspec.sql.builder._select import SelectBuilder
from sqlspec.sql.builder._update import UpdateBuilder
from sqlspec.sql.statement import ValidationResult, validate_sql

__all__ = (
    "DeleteBuilder",
    "InsertBuilder",
    "MergeBuilder",
    "QueryBuilder",
    "SQLBuilderError",
    "SafeQuery",
    "SelectBuilder",
    "UpdateBuilder",
    "ValidationResult",
    "delete",
    "insert",
    "merge",
    "select",
    "update",
    "validate_sql",
)


def select(*columns: Union[str, exp.Expression], dialect: Optional[DialectType] = None) -> SelectBuilder:
    """Create a SELECT builder.

    Args:
        *columns: Optional columns to select. If not provided, selects all columns.
        dialect: Optional SQL dialect to use for the query.

    Returns:
        SelectBuilder: A new SelectBuilder instance with the specified columns.
    """
    builder = SelectBuilder(dialect=dialect)
    if columns:
        builder.select(*columns)
    return builder


def insert(dialect: Optional[DialectType] = None) -> InsertBuilder:
    """Create an INSERT builder.

    Args:
        dialect: Optional SQL dialect to use for the query.

    Returns:
        InsertBuilder: A new InsertBuilder instance.
    """
    return InsertBuilder(dialect=dialect)


def update(table: Optional[str] = None, dialect: Optional[DialectType] = None) -> UpdateBuilder:
    """Create an UPDATE builder.

    Args:
        table: Optional table name to update. If provided, sets the target table for the UPDATE.
        dialect: Optional SQL dialect to use for the query.

    Returns:
        UpdateBuilder: A new UpdateBuilder instance, optionally with the target table set.
    """
    builder = UpdateBuilder(dialect=dialect)
    if table:
        builder.table(table)
    return builder


def delete(dialect: Optional[DialectType] = None) -> DeleteBuilder:
    """Create a DELETE builder.

    Args:
        dialect: Optional SQL dialect to use for the query.

    Returns:
        DeleteBuilder: A new DeleteBuilder instance.

    """
    return DeleteBuilder(dialect=dialect)


def merge(dialect: Optional[DialectType] = None) -> MergeBuilder:
    """Create a new MERGE query builder.

    Args:
        dialect: Optional SQL dialect to use for the query.

    Returns:
        MergeBuilder: A new MergeBuilder instance.
    """
    return MergeBuilder(dialect=dialect)
