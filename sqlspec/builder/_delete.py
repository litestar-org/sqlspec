"""DELETE statement builder.

Provides a fluent interface for building SQL DELETE queries with
parameter binding and validation.
"""

from typing import TYPE_CHECKING, Any

from sqlglot import exp

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

from sqlspec.builder._base import BuiltQuery, QueryBuilder
from sqlspec.builder._dml import DeleteFromClauseMixin
from sqlspec.builder._explain import ExplainMixin
from sqlspec.builder._select import ReturningClauseMixin, WhereClauseMixin
from sqlspec.core import SQLResult
from sqlspec.exceptions import SQLBuilderError

__all__ = ("Delete",)


class Delete(QueryBuilder, WhereClauseMixin, ReturningClauseMixin, DeleteFromClauseMixin, ExplainMixin):
    """Builder for DELETE statements.

    Constructs SQL DELETE statements with parameter binding and validation.
    Does not support JOIN operations to maintain cross-dialect compatibility.
    """

    __slots__ = ()
    _expression: exp.Expression | None

    def __init__(self, table: str | None = None, **kwargs: Any) -> None:
        """Initialize DELETE with optional table.

        Args:
            table: Target table name
            **kwargs: Additional QueryBuilder arguments
        """
        (dialect, schema, enable_optimization, optimize_joins, optimize_predicates, simplify_expressions) = (
            self._parse_query_builder_kwargs(kwargs)
        )
        super().__init__(
            dialect=dialect,
            schema=schema,
            enable_optimization=enable_optimization,
            optimize_joins=optimize_joins,
            optimize_predicates=optimize_predicates,
            simplify_expressions=simplify_expressions,
        )
        self._initialize_expression()

        if table:
            self.from_(table)

    @property
    def _expected_result_type(self) -> "type[SQLResult]":
        """Get the expected result type for DELETE operations.

        Returns:
            The ExecuteResult type for DELETE statements.
        """
        return SQLResult

    def _create_base_expression(self) -> "exp.Delete":
        """Create a new sqlglot Delete expression.

        Returns:
            A new sqlglot Delete expression.
        """
        return exp.Delete()

    def build(self, dialect: "DialectType" = None) -> "BuiltQuery":
        """Build the DELETE query with validation.

        Args:
            dialect: Optional dialect override for SQL generation.

        Returns:
            BuiltQuery: The built query with SQL and parameters.

        Raises:
            SQLBuilderError: If the table is not specified.
        """

        if self._expression is None or not isinstance(self._expression, exp.Delete):
            msg = "DELETE requires a table to be specified. Use from() to set the table."
            raise SQLBuilderError(msg)

        if self._expression.this is None:
            msg = "DELETE requires a table to be specified. Use from() to set the table."
            raise SQLBuilderError(msg)

        return super().build(dialect=dialect)
