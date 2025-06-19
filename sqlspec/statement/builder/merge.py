"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

from dataclasses import dataclass

from sqlglot import exp

from sqlspec.statement.builder.base import QueryBuilder
from sqlspec.statement.builder.mixins import (
    MergeIntoClauseMixin,
    MergeMatchedClauseMixin,
    MergeNotMatchedBySourceClauseMixin,
    MergeNotMatchedClauseMixin,
    MergeOnClauseMixin,
    MergeUsingClauseMixin,
)
from sqlspec.statement.result import SQLResult
from sqlspec.typing import RowT

__all__ = ("MergeBuilder",)


@dataclass(unsafe_hash=True)
class MergeBuilder(
    QueryBuilder[RowT],
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

    Example:
        ```python
        # Basic MERGE statement
        merge_query = (
            MergeBuilder()
            .into("target_table")
            .using("source_table", "src")
            .on("target_table.id = src.id")
            .when_matched_then_update(
                {"name": "src.name", "updated_at": "NOW()"}
            )
            .when_not_matched_then_insert(
                columns=["id", "name", "created_at"],
                values=["src.id", "src.name", "NOW()"],
            )
        )

        # MERGE with subquery source
        source_query = (
            SelectBuilder()
            .select("id", "name", "email")
            .from_("temp_users")
            .where("status = 'pending'")
        )

        merge_query = (
            MergeBuilder()
            .into("users")
            .using(source_query, "src")
            .on("users.email = src.email")
            .when_matched_then_update({"name": "src.name"})
            .when_not_matched_then_insert(
                columns=["id", "name", "email"],
                values=["src.id", "src.name", "src.email"],
            )
        )
        ```
    """

    @property
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
        """Return the expected result type for this builder.

        Returns:
            The SQLResult type for MERGE statements.
        """
        return SQLResult[RowT]

    def _create_base_expression(self) -> "exp.Merge":
        """Create a base MERGE expression.

        Returns:
            A new sqlglot Merge expression with empty clauses.
        """
        return exp.Merge(this=None, using=None, on=None, whens=exp.Whens(expressions=[]))
