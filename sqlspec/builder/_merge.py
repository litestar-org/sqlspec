"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

from dataclasses import dataclass

from sqlglot import exp

from sqlspec.builder._base import QueryBuilder
from sqlspec.builder.mixins import (
    MergeIntoClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeOnClauseMixin,
    MergeUsingClauseMixin,
)
from sqlspec.core.result import SQLResult

__all__ = ("Merge",)


@dataclass(unsafe_hash=True)
class Merge(
    QueryBuilder,
    MergeUsingClauseMixin,
    MergeOnClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeIntoClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
):
    """Builder for MERGE statements.

    This builder provides a fluent interface for constructing SQL MERGE statements
    (also known as UPSERT in some databases) with automatic parameter binding and validation.
    """

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Return the expected result type for this builder.

        Returns:
            The SQLResult type for MERGE statements.
        """
        return SQLResult

    def _create_base_expression(self) -> "exp.Merge":
        """Create a base MERGE expression.

        Returns:
            A new sqlglot Merge expression with empty clauses.
        """
        return exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
