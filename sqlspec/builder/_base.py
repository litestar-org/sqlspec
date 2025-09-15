"""Base query builder with validation and parameter binding.

Provides abstract base classes and core functionality for SQL query builders.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, NoReturn, Optional, Union, cast

import sqlglot
from sqlglot import Dialect, exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError as SQLGlotParseError
from sqlglot.optimizer import optimize
from typing_extensions import Self

from sqlspec.core.cache import get_cache, get_cache_config
from sqlspec.core.hashing import hash_optimized_expression
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_expression_and_parameters, has_sql_method, has_with_method

if TYPE_CHECKING:
    from sqlspec.core.result import SQLResult

__all__ = ("QueryBuilder", "SafeQuery")

logger = get_logger(__name__)


class SafeQuery:
    """SQL query with bound parameters."""

    __slots__ = ("dialect", "parameters", "sql")

    def __init__(
        self, sql: str, parameters: Optional[dict[str, Any]] = None, dialect: Optional[DialectType] = None
    ) -> None:
        self.sql = sql
        self.parameters = parameters if parameters is not None else {}
        self.dialect = dialect


class QueryBuilder(ABC):
    """Abstract base class for SQL query builders.

    Provides common functionality for dialect handling, parameter management,
    and query construction using SQLGlot.
    """

    __slots__ = (
        "_expression",
        "_parameter_counter",
        "_parameters",
        "_with_ctes",
        "dialect",
        "enable_optimization",
        "optimize_joins",
        "optimize_predicates",
        "schema",
        "simplify_expressions",
    )

    def __init__(
        self,
        dialect: Optional[DialectType] = None,
        schema: Optional[dict[str, dict[str, str]]] = None,
        enable_optimization: bool = True,
        optimize_joins: bool = True,
        optimize_predicates: bool = True,
        simplify_expressions: bool = True,
    ) -> None:
        self.dialect = dialect
        self.schema = schema
        self.enable_optimization = enable_optimization
        self.optimize_joins = optimize_joins
        self.optimize_predicates = optimize_predicates
        self.simplify_expressions = simplify_expressions

        self._expression: Optional[exp.Expression] = None
        self._parameters: dict[str, Any] = {}
        self._parameter_counter: int = 0
        self._with_ctes: dict[str, exp.CTE] = {}

    def _initialize_expression(self) -> None:
        """Initialize the base expression. Called after __init__."""
        self._expression = self._create_base_expression()
        if not self._expression:
            self._raise_sql_builder_error(
                "QueryBuilder._create_base_expression must return a valid sqlglot expression."
            )

    def get_expression(self) -> Optional[exp.Expression]:
        """Get expression reference (no copy).

        Returns:
            The current SQLGlot expression or None if not set
        """
        return self._expression

    def set_expression(self, expression: exp.Expression) -> None:
        """Set expression with validation.

        Args:
            expression: SQLGlot expression to set

        Raises:
            TypeError: If expression is not a SQLGlot Expression
        """
        if not isinstance(expression, exp.Expression):
            msg = f"Expected Expression, got {type(expression)}"
            raise TypeError(msg)
        self._expression = expression

    def has_expression(self) -> bool:
        """Check if expression exists.

        Returns:
            True if expression is set, False otherwise
        """
        return self._expression is not None

    @abstractmethod
    def _create_base_expression(self) -> exp.Expression:
        """Create the base sqlglot expression for the specific query type.

        Returns:
            A new sqlglot expression appropriate for the query type.
        """

    @property
    @abstractmethod
    def _expected_result_type(self) -> "type[SQLResult]":
        """The expected result type for the query being built.

        Returns:
            type[ResultT]: The type of the result.
        """

    @staticmethod
    def _raise_sql_builder_error(message: str, cause: Optional[BaseException] = None) -> NoReturn:
        """Helper to raise SQLBuilderError, potentially with a cause.

        Args:
            message: The error message.
            cause: The optional original exception to chain.

        Raises:
            SQLBuilderError: Always raises this exception.
        """
        raise SQLBuilderError(message) from cause

    def _add_parameter(self, value: Any, context: Optional[str] = None) -> str:
        """Adds a parameter to the query and returns its placeholder name.

        Args:
            value: The value of the parameter.
            context: Optional context hint for parameter naming (e.g., "where", "join")

        Returns:
            str: The placeholder name for the parameter (e.g., :param_1 or :where_param_1).
        """
        self._parameter_counter += 1

        param_name = f"{context}_param_{self._parameter_counter}" if context else f"param_{self._parameter_counter}"

        self._parameters[param_name] = value
        return param_name

    def _parameterize_expression(self, expression: exp.Expression) -> exp.Expression:
        """Replace literal values in an expression with bound parameters.

        This method traverses a SQLGlot expression tree and replaces literal
        values with parameter placeholders, adding the values to the builder's
        parameter collection.

        Args:
            expression: The SQLGlot expression to parameterize

        Returns:
            A new expression with literals replaced by parameter placeholders
        """

        def replacer(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Literal):
                if node.this in {True, False, None}:
                    return node
                param_name = self._add_parameter(node.this, context="where")
                return exp.Placeholder(this=param_name)
            return node

        return expression.transform(replacer, copy=False)

    def add_parameter(self: Self, value: Any, name: Optional[str] = None) -> tuple[Self, str]:
        """Explicitly adds a parameter to the query.

        This is useful for parameters that are not directly tied to a
        builder method like `where` or `values`.

        Args:
            value: The value of the parameter.
            name: Optional explicit name for the parameter. If None, a name
                  will be generated.

        Returns:
            tuple[Self, str]: The builder instance and the parameter name.
        """
        if name:
            if name in self._parameters:
                self._raise_sql_builder_error(f"Parameter name '{name}' already exists.")
            self._parameters[name] = value
            return self, name

        self._parameter_counter += 1
        param_name = f"param_{self._parameter_counter}"
        self._parameters[param_name] = value
        return self, param_name

    def _generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name when collision occurs.

        Args:
            base_name: The desired base name for the parameter

        Returns:
            A unique parameter name that doesn't exist in current parameters
        """
        if base_name not in self._parameters:
            return base_name

        for i in range(1, 1000):
            name = f"{base_name}_{i}"
            if name not in self._parameters:
                return name

        import uuid

        return f"{base_name}_{uuid.uuid4().hex[:8]}"

    def _merge_cte_parameters(self, cte_name: str, parameters: dict[str, Any]) -> dict[str, str]:
        """Merge CTE parameters with unique naming to prevent collisions.

        Args:
            cte_name: The name of the CTE for parameter prefixing
            parameters: The CTE's parameter dictionary

        Returns:
            Mapping of old parameter names to new unique names
        """
        param_mapping = {}
        for old_name, value in parameters.items():
            new_name = self._generate_unique_parameter_name(f"{cte_name}_{old_name}")
            param_mapping[old_name] = new_name
            self.add_parameter(value, name=new_name)
        return param_mapping

    def _update_placeholders_in_expression(
        self, expression: exp.Expression, param_mapping: dict[str, str]
    ) -> exp.Expression:
        """Update parameter placeholders in expression to use new names.

        Args:
            expression: The SQLGlot expression to update
            param_mapping: Mapping of old parameter names to new names

        Returns:
            Updated expression with new placeholder names
        """

        def placeholder_replacer(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Placeholder) and str(node.this) in param_mapping:
                return exp.Placeholder(this=param_mapping[str(node.this)])
            return node

        return expression.transform(placeholder_replacer, copy=False)

    def _generate_builder_cache_key(self, config: "Optional[StatementConfig]" = None) -> str:
        """Generate cache key based on builder state and configuration.

        Args:
            config: Optional SQL configuration that affects the generated SQL

        Returns:
            A unique cache key representing the builder state and configuration
        """
        import hashlib

        dialect_name: str = self.dialect_name or "default"

        if self._expression is None:
            self._expression = self._create_base_expression()

        expr_sql: str = self._expression.sql() if self._expression else "None"

        state_parts = [
            f"expression:{expr_sql}",
            f"parameters:{sorted(self._parameters.items())}",
            f"ctes:{sorted(self._with_ctes.keys())}",
            f"dialect:{dialect_name}",
            f"schema:{self.schema}",
            f"optimization:{self.enable_optimization}",
            f"optimize_joins:{self.optimize_joins}",
            f"optimize_predicates:{self.optimize_predicates}",
            f"simplify_expressions:{self.simplify_expressions}",
        ]

        if config:
            config_parts = [
                f"config_dialect:{config.dialect or 'default'}",
                f"enable_parsing:{config.enable_parsing}",
                f"enable_validation:{config.enable_validation}",
                f"enable_transformations:{config.enable_transformations}",
                f"enable_analysis:{config.enable_analysis}",
                f"enable_caching:{config.enable_caching}",
                f"param_style:{config.parameter_config.default_parameter_style.value}",
            ]
            state_parts.extend(config_parts)

        state_string = "|".join(state_parts)
        return f"builder:{hashlib.sha256(state_string.encode()).hexdigest()[:16]}"

    def with_cte(self: Self, alias: str, query: "Union[QueryBuilder, exp.Select, str]") -> Self:
        """Adds a Common Table Expression (CTE) to the query.

        Args:
            alias: The alias for the CTE.
            query: The CTE query, which can be another QueryBuilder instance,
                   a raw SQL string, or a sqlglot Select expression.

        Returns:
            Self: The current builder instance for method chaining.
        """
        if alias in self._with_ctes:
            self._raise_sql_builder_error(f"CTE with alias '{alias}' already exists.")

        cte_select_expression: exp.Select

        if isinstance(query, QueryBuilder):
            query_expr = query.get_expression()
            if query_expr is None:
                self._raise_sql_builder_error("CTE query builder has no expression.")
            if not isinstance(query_expr, exp.Select):
                msg = f"CTE query builder expression must be a Select, got {type(query_expr).__name__}."
                self._raise_sql_builder_error(msg)
            cte_select_expression = query_expr
            param_mapping = self._merge_cte_parameters(alias, query.parameters)
            updated_expression = self._update_placeholders_in_expression(cte_select_expression, param_mapping)
            if not isinstance(updated_expression, exp.Select):
                msg = f"Updated CTE expression must be a Select, got {type(updated_expression).__name__}."
                self._raise_sql_builder_error(msg)
            cte_select_expression = updated_expression

        elif isinstance(query, str):
            try:
                parsed_expression = sqlglot.parse_one(query, read=self.dialect_name)
                if not isinstance(parsed_expression, exp.Select):
                    msg = f"CTE query string must parse to a SELECT statement, got {type(parsed_expression).__name__}."
                    self._raise_sql_builder_error(msg)
                cte_select_expression = parsed_expression
            except SQLGlotParseError as e:
                self._raise_sql_builder_error(f"Failed to parse CTE query string: {e!s}", e)
            except Exception as e:
                msg = f"An unexpected error occurred while parsing CTE query string: {e!s}"
                self._raise_sql_builder_error(msg, e)
        elif isinstance(query, exp.Select):
            cte_select_expression = query
        else:
            msg = f"Invalid query type for CTE: {type(query).__name__}"
            self._raise_sql_builder_error(msg)

        self._with_ctes[alias] = exp.CTE(this=cte_select_expression, alias=exp.to_table(alias))
        return self

    def build(self) -> "SafeQuery":
        """Builds the SQL query string and parameters.

        Returns:
            SafeQuery: A dataclass containing the SQL string and parameters.
        """
        if self._expression is None:
            self._raise_sql_builder_error("QueryBuilder expression not initialized.")

        if self._with_ctes:
            final_expression = self._expression
            if has_with_method(final_expression):
                for alias, cte_node in self._with_ctes.items():
                    final_expression = cast("Any", final_expression).with_(cte_node.args["this"], as_=alias, copy=False)
            elif isinstance(final_expression, (exp.Select, exp.Insert, exp.Update, exp.Delete, exp.Union)):
                final_expression = exp.With(expressions=list(self._with_ctes.values()), this=final_expression)
        else:
            final_expression = self._expression

        if self.enable_optimization and isinstance(final_expression, exp.Expression):
            final_expression = self._optimize_expression(final_expression)

        try:
            if has_sql_method(final_expression):
                sql_string = final_expression.sql(dialect=self.dialect_name, pretty=True)
            else:
                sql_string = str(final_expression)
        except Exception as e:
            err_msg = f"Error generating SQL from expression: {e!s}"
            logger.exception("SQL generation failed")
            self._raise_sql_builder_error(err_msg, e)

        return SafeQuery(sql=sql_string, parameters=self._parameters.copy(), dialect=self.dialect)

    def _optimize_expression(self, expression: exp.Expression) -> exp.Expression:
        """Apply SQLGlot optimizations to the expression.

        Args:
            expression: The expression to optimize

        Returns:
            The optimized expression
        """
        if not self.enable_optimization:
            return expression

        optimizer_settings = {
            "optimize_joins": self.optimize_joins,
            "pushdown_predicates": self.optimize_predicates,
            "simplify_expressions": self.simplify_expressions,
        }

        dialect_name = self.dialect_name or "default"
        cache_key = hash_optimized_expression(
            expression, dialect=dialect_name, schema=self.schema, optimizer_settings=optimizer_settings
        )

        cache = get_cache()
        cached_optimized = cache.get("optimized", cache_key)
        if cached_optimized:
            return cast("exp.Expression", cached_optimized)

        try:
            optimized = optimize(
                expression, schema=self.schema, dialect=self.dialect_name, optimizer_settings=optimizer_settings
            )

            cache.put("optimized", cache_key, optimized)

        except Exception:
            return expression
        else:
            return optimized

    def to_statement(self, config: "Optional[StatementConfig]" = None) -> "SQL":
        """Converts the built query into a SQL statement object.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        cache_config = get_cache_config()
        if not cache_config.compiled_cache_enabled:
            return self._to_statement(config)

        cache_key_str = self._generate_builder_cache_key(config)

        cache = get_cache()
        cached_sql = cache.get("builder", cache_key_str)
        if cached_sql is not None:
            return cast("SQL", cached_sql)

        sql_statement = self._to_statement(config)
        cache.put("builder", cache_key_str, sql_statement)

        return sql_statement

    def _to_statement(self, config: "Optional[StatementConfig]" = None) -> "SQL":
        """Internal method to create SQL statement.

        Args:
            config: Optional SQL configuration.

        Returns:
            SQL: A SQL statement object.
        """
        safe_query = self.build()

        if isinstance(safe_query.parameters, dict):
            kwargs = safe_query.parameters
            parameters: Optional[tuple[Any, ...]] = None
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
            config = StatementConfig(
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
                dialect=safe_query.dialect,
            )

        sql_string = safe_query.sql
        if (
            config.dialect is not None
            and config.dialect != safe_query.dialect
            and self._expression is not None
            and has_sql_method(self._expression)
        ):
            try:
                sql_string = self._expression.sql(dialect=config.dialect, pretty=True)
            except Exception:
                sql_string = safe_query.sql

        if kwargs:
            return SQL(sql_string, statement_config=config, **kwargs)
        if parameters:
            return SQL(sql_string, *parameters, statement_config=config)
        return SQL(sql_string, statement_config=config)

    def __str__(self) -> str:
        """Return the SQL string representation of the query.

        Returns:
            str: The SQL string for this query.
        """
        return self.build().sql

    @property
    def dialect_name(self) -> "Optional[str]":
        """Returns the name of the dialect, if set."""
        if isinstance(self.dialect, str):
            return self.dialect
        if self.dialect is None:
            return None
        if isinstance(self.dialect, type) and issubclass(self.dialect, Dialect):
            return self.dialect.__name__.lower()
        if isinstance(self.dialect, Dialect):
            return type(self.dialect).__name__.lower()
        return getattr(self.dialect, "__name__", str(self.dialect)).lower()

    def _merge_sql_object_parameters(self, sql_obj: Any) -> None:
        """Merge parameters from a SQL object into the builder.

        Args:
            sql_obj: Object with parameters attribute containing parameter mappings
        """
        if not has_expression_and_parameters(sql_obj):
            return

        sql_parameters = getattr(sql_obj, "parameters", {})
        for param_name, param_value in sql_parameters.items():
            unique_name = self._generate_unique_parameter_name(param_name)
            self.add_parameter(param_value, name=unique_name)

    @property
    def parameters(self) -> dict[str, Any]:
        """Public access to query parameters."""
        return self._parameters

    def set_parameters(self, parameters: dict[str, Any]) -> None:
        """Set query parameters (public API)."""
        self._parameters = parameters.copy()

    @property
    def with_ctes(self) -> "dict[str, exp.CTE]":
        """Get WITH clause CTEs (public API)."""
        return dict(self._with_ctes)

    def generate_unique_parameter_name(self, base_name: str) -> str:
        """Generate unique parameter name (public API)."""
        return self._generate_unique_parameter_name(base_name)
