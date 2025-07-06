"""Safe SQL query builder with validation and parameter binding.

This module provides a fluent interface for building SQL queries safely,
with automatic parameter binding and validation.
"""

from dataclasses import dataclass, field
from typing import Any, Optional

from sqlglot import exp

from sqlspec.statement.builder._base import QueryBuilder, SafeQuery
from sqlspec.statement.builder.mixins import DeleteFromClauseMixin, ReturningClauseMixin, WhereClauseMixin
from sqlspec.statement.result import SQLResult
from sqlspec.typing import RowT

__all__ = ("Delete",)


@dataclass(unsafe_hash=True)
class Delete(QueryBuilder[RowT], WhereClauseMixin, ReturningClauseMixin, DeleteFromClauseMixin):
    """Builder for DELETE statements.

    This builder provides a fluent interface for constructing SQL DELETE statements
    with automatic parameter binding and validation. It does not support JOIN
    operations to maintain cross-dialect compatibility and safety.

    Example:
        ```python
        # Basic DELETE
        delete_query = Delete().from_("users").where("age < 18")

        # Even more concise with constructor
        delete_query = Delete("users").where("age < 18")

        # DELETE with parameterized conditions
        delete_query = (
            Delete()
            .from_("users")
            .where_eq("status", "inactive")
            .where_in("category", ["test", "demo"])
        )
        ```
    """

    _table: "Optional[str]" = field(default=None, init=False)

    def __init__(self, table: Optional[str] = None, **kwargs: Any) -> None:
        """Initialize DELETE with optional table.

        Args:
            table: Target table name
            **kwargs: Additional QueryBuilder arguments
        """
        super().__init__(**kwargs)

        # Initialize fields from dataclass
        self._table = None

        if table:
            self.from_(table)

    @property
    def _expected_result_type(self) -> "type[SQLResult[RowT]]":
        """Get the expected result type for DELETE operations.

        Returns:
            The ExecuteResult type for DELETE statements.
        """
        return SQLResult[RowT]

    def _create_base_expression(self) -> "exp.Delete":
        """Create a new sqlglot Delete expression.

        Returns:
            A new sqlglot Delete expression.
        """
        return exp.Delete()

    def build(self) -> "SafeQuery":
        """Build the DELETE query with validation.

        Returns:
            SafeQuery: The built query with SQL and parameters.

        Raises:
            SQLBuilderError: If the table is not specified.
        """

        if not self._table:
            from sqlspec.exceptions import SQLBuilderError

            msg = "DELETE requires a table to be specified. Use from() to set the table."
            raise SQLBuilderError(msg)

        return super().build()
