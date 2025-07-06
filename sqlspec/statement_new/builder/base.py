"""Base query builder."""

from abc import ABC
from typing import TYPE_CHECKING, Any, Generic, Optional

from sqlglot.dialects.dialect import DialectType

from sqlspec.statement_new.builder.mixins import (
    AggregationMixin,
    CoreQueryMixin,
    DatabaseSpecificMixin,
    JoinOperationsMixin,
)
from sqlspec.statement_new.result import SQLResult
from sqlspec.statement_new.sql import SQL, SQLConfig
from sqlspec.typing import RowT

if TYPE_CHECKING:
    from sqlglot import exp

__all__ = ("BaseBuilder", "QueryBuilder", )


class QueryBuilder(ABC, Generic[RowT]):
    """Abstract base class for query builders."""

    _expression: Optional["exp.Expression"] = None
    _parameters: dict[str, Any]
    _parameter_counter: int
    dialect: DialectType

    def build(self) -> SQLResult[RowT]:
        """Build the query into a result."""
        raise NotImplementedError

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[str, str]:
        """Add a parameter to the query."""
        raise NotImplementedError

    def _parameterize_expression(self, expression: "exp.Expression") -> "exp.Expression":
        """Parameterize an expression."""
        raise NotImplementedError


class BaseBuilder(QueryBuilder[RowT], CoreQueryMixin, JoinOperationsMixin, AggregationMixin, DatabaseSpecificMixin):
    """Base builder with all mixins integrated."""

    def __init__(
        self, expression: Optional["exp.Expression"] = None, dialect: DialectType = None, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._expression = expression
        self.dialect = dialect
        self._parameters: dict[str, Any] = {}
        self._parameter_counter = 0

    def to_statement(self, config: Optional[SQLConfig] = None) -> SQL:
        """Converts the built query into a SQL statement object.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        safe_query = self.build()

        if isinstance(safe_query.parameters, dict):
            kwargs = safe_query.parameters
            parameters = None
        else:
            kwargs = None
            parameters = (
                safe_query.parameters
                if isinstance(safe_query.parameters, tuple)
                else tuple(safe_query.parameters)
                if safe_query.parameters
                else None
            )

        if config is None:
            config = SQLConfig(dialect=safe_query.dialect)

        if kwargs:
            return SQL(safe_query.sql, config=config, **kwargs)
        if parameters:
            return SQL(safe_query.sql, *parameters, config=config)
        return SQL(safe_query.sql, config=config)

    def __str__(self) -> str:
        """Return the SQL string representation of the query.

        Returns:
            str: The SQL string for this query.
        """
        try:
            return self.build().sql
        except Exception:
            return super().__str__()

    @property
    def dialect_name(self) -> Optional[str]:
        """Returns the name of the dialect, if set."""
        return self.dialect if isinstance(self.dialect, str) else None

    @property
    def parameters(self) -> dict[str, Any]:
        """Public access to query parameters."""
        return self._parameters

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[str, str]:
        """Add a parameter to the query."""
        if name is None:
            name = f"p{self._parameter_counter}"
            self._parameter_counter += 1
        self._parameters[name] = value
        return (name, name)

    def _parameterize_expression(self, expression: "exp.Expression") -> "exp.Expression":
        """Parameterize an expression."""
        # TODO: Implement expression parameterization
        return expression

    def build(self) -> SQLResult[RowT]:
        """Build the query into a result."""
        if self._expression is None:
            msg = "No expression to build"
            raise ValueError(msg)

        sql_str = self._expression.sql(dialect=self.dialect)

        # Return a result object with sql and parameters
        class BuildResult:
            def __init__(self, sql: str, parameters: dict[str, Any], dialect: DialectType) -> None:
                self.sql = sql
                self.parameters = parameters
                self.dialect = dialect

        return BuildResult(sql_str, self._parameters, self.dialect)  # type: ignore[return-value]
