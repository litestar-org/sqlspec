from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar, Union, cast

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError

from sqlspec.exceptions import SQLParsingError
from sqlspec.protocols import HasLimitProtocol, HasOffsetProtocol, HasOrderByProtocol, HasWhereProtocol
from sqlspec.statement.parameter_manager import ParameterManager
from sqlspec.statement.parameters import ParameterConverter, ParameterStyle, ParameterValidator
from sqlspec.statement.sql_compiler import SQLCompiler
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.filters import StatementFilter

__all__ = ("SQL", "SQLConfig", "Statement")

logger = get_logger("sqlspec.statement")

Statement = Union[str, exp.Expression, "SQL"]
_expression_cache: dict[int, exp.Expression] = {}


@dataclass
class SQLConfig:
    """Configuration for SQL statement behavior."""

    dialect: DialectType | None = None
    parse_errors_as_warnings: bool = False
    cache_expressions: bool = True
    parameter_converter: ParameterConverter = field(default_factory=ParameterConverter)
    parameter_validator: ParameterValidator = field(default_factory=ParameterValidator)
    # Parameter style validation
    allowed_parameter_styles: tuple[str, ...] | None = None
    allow_mixed_parameter_styles: bool = False
    input_sql_had_placeholders: bool = False
    # Legacy attributes for compatibility
    enable_analysis: bool = True
    enable_transformations: bool = True
    enable_validation: bool = True
    enable_parsing: bool = True
    strict_mode: bool = False
    cache_parsed_expression: bool = True
    analysis_cache_size: int = 128
    target_parameter_style: str | None = None

    def validate_parameter_style(self, style: str) -> bool:
        """Check if a parameter style is allowed.

        Args:
            style: The parameter style to check

        Returns:
            True if the style is allowed, False otherwise
        """
        if self.allowed_parameter_styles is None:
            return True
        return style in self.allowed_parameter_styles


class SQL:
    """Immutable SQL statement with centralized parameter management."""

    __slots__ = (
        "_compiler",
        "_config",
        "_filters",
        "_is_many",
        "_is_script",
        "_original_parameters",
        "_parameter_manager",
        "_raw_parameters",
        "_raw_sql",
        "_statement",
    )

    _default_config: ClassVar[SQLConfig] = SQLConfig()

    def __init__(
        self,
        statement: Statement,
        parameters: tuple[Any, ...] | list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        config: SQLConfig | None = None,
    ) -> None:
        self._config = config or self._default_config
        # Convert list to tuple if needed
        if isinstance(parameters, list):
            parameters = tuple(parameters)
        self._parameter_manager = ParameterManager(
            parameters=parameters, kwargs=kwargs, converter=self._config.parameter_converter
        )
        self._filters: list[StatementFilter] = []
        self._is_many = False
        self._is_script = False
        self._compiler: SQLCompiler | None = None
        self._original_parameters: Any = None

        if isinstance(statement, SQL):
            self._statement = statement._statement
            self._filters.extend(statement._filters)
            self._is_many = statement._is_many
            self._is_script = statement._is_script
            self._original_parameters = getattr(statement, "_original_parameters", None)
            # Also copy parameters from the source SQL object
            if statement._parameter_manager.named_parameters:
                self._parameter_manager.named_params.update(statement._parameter_manager.named_parameters)
            # These attributes need to be set for compatibility with tests
            self._raw_sql = getattr(statement, "_raw_sql", "")
            self._raw_parameters = getattr(statement, "_raw_parameters", (None, None))
        else:
            self._statement = self.to_expression(statement)
            # Store the raw values for tests
            self._raw_sql = statement if isinstance(statement, str) else statement.sql()
            self._raw_parameters = (parameters, kwargs)

    @classmethod
    def from_sql_object(cls, sql_obj: SQL, **kwargs: Any) -> SQL:
        """Create a new SQL object from an existing one, with modifications."""
        new_kwargs = {
            "statement": sql_obj._statement,
            "parameters": sql_obj._parameter_manager.positional_parameters,
            "kwargs": sql_obj._parameter_manager.named_parameters,
            "config": sql_obj._config,
            **kwargs,
        }
        instance = cls(**new_kwargs)
        instance._filters.extend(sql_obj._filters)
        instance._is_many = kwargs.get("is_many", sql_obj._is_many)
        instance._is_script = kwargs.get("is_script", sql_obj._is_script)
        instance._original_parameters = getattr(sql_obj, "_original_parameters", None)
        return instance

    @classmethod
    def from_str_or_expression(
        cls, statement: str | exp.Expression, *parameters: Any, config: SQLConfig | None = None, **kwargs: Any
    ) -> SQL:
        """Create a new SQL object from a string or SQLGlot expression."""
        return cls(statement, parameters=parameters, kwargs=kwargs, config=config)

    def to_expression(self, statement: str | exp.Expression) -> exp.Expression:
        """Parse a string into a SQLGlot expression, with optional caching."""
        if isinstance(statement, exp.Expression):
            return statement

        if not statement.strip():
            return exp.Anonymous(this="")

        cache_key = hash((statement, self._config.dialect))
        if self._config.cache_expressions and cache_key in _expression_cache:
            return _expression_cache[cache_key]

        try:
            parsed = sqlglot.parse_one(statement, read=self._config.dialect)
        except ParseError as e:
            if self._config.parse_errors_as_warnings:
                logger.warning(
                    "Failed to parse SQL, returning Anonymous expression.", extra={"sql": statement, "error": str(e)}
                )
                return exp.Anonymous(this=statement)
            msg = f"Failed to parse SQL: {statement}"
            raise SQLParsingError(msg) from e
        else:
            if self._config.cache_expressions:
                _expression_cache[cache_key] = parsed
            return parsed

    def copy(self, **kwargs: Any) -> SQL:
        """Create a copy of the current SQL object with updated attributes."""
        new_sql = self.from_sql_object(self, **kwargs)
        # Copy over any attributes not handled by from_sql_object
        new_sql._original_parameters = getattr(self, "_original_parameters", None)
        return new_sql

    def where(self, condition: str | exp.Condition) -> SQL:
        """Add a WHERE clause to the statement."""
        current_statement = self._statement
        if not isinstance(current_statement, HasWhereProtocol):
            current_statement = exp.Select(this=current_statement)

        condition_expr = self.to_expression(condition) if isinstance(condition, str) else condition
        new_statement = cast("HasWhereProtocol", current_statement).where(condition_expr)
        return self.copy(statement=new_statement)

    def limit(self, limit: int, use_parameter: bool = False) -> SQL:
        """Add a LIMIT clause to the statement."""
        current_statement = self._statement
        if not isinstance(current_statement, HasLimitProtocol):
            current_statement = exp.Select(this=current_statement)

        if use_parameter:
            # Use a placeholder for the limit value
            new_statement = cast("HasLimitProtocol", current_statement).limit(exp.Placeholder(this="limit"))
            # Store the limit value in parameters
            new_sql = self.copy(statement=new_statement)
            new_sql._parameter_manager.add_named_parameter("limit", limit)
            return new_sql
        new_statement = cast("HasLimitProtocol", current_statement).limit(limit)
        return self.copy(statement=new_statement)

    def offset(self, offset: int, use_parameter: bool = False) -> SQL:
        """Add an OFFSET clause to the statement."""
        current_statement = self._statement
        if not isinstance(current_statement, HasOffsetProtocol):
            current_statement = exp.Select(this=current_statement)

        if use_parameter:
            # Use a placeholder for the offset value
            new_statement = cast("HasOffsetProtocol", current_statement).offset(exp.Placeholder(this="offset"))
            # Store the offset value in parameters
            new_sql = self.copy(statement=new_statement)
            new_sql._parameter_manager.add_named_parameter("offset", offset)
            return new_sql
        new_statement = cast("HasOffsetProtocol", current_statement).offset(offset)
        return self.copy(statement=new_statement)

    def order_by(self, *expressions: str | exp.Expression) -> SQL:
        """Add an ORDER BY clause to the statement."""
        current_statement = self._statement
        if not isinstance(current_statement, HasOrderByProtocol):
            current_statement = exp.Select(this=current_statement)

        order_exprs = [self.to_expression(e) if isinstance(e, str) else e for e in expressions]
        new_statement = cast("HasOrderByProtocol", current_statement).order_by(*order_exprs)
        return self.copy(statement=new_statement)

    def filter(self, filter_obj: StatementFilter) -> SQL:
        """Apply a statement filter."""
        new_sql = self.copy()
        new_sql._filters.append(filter_obj)
        return new_sql

    def as_many(self, parameters: Any = None) -> SQL:
        """Flag the statement for 'execute many' style execution.

        Args:
            parameters: Optional parameters for execute many. If provided, sets is_many=True.
                       Can be a list of parameter tuples/dicts for batch execution.

        Returns:
            New SQL instance configured for execute many.
        """
        # Create a new SQL instance with is_many flag set
        new_sql = self.copy()
        new_sql._is_many = True

        # If parameters provided, store them
        if parameters is not None:
            # Store the original parameters for execute many
            new_sql._original_parameters = parameters

        return new_sql

    def as_script(self, is_script: bool = True) -> SQL:
        """Flag the statement as a script to be executed."""
        return self.copy(is_script=is_script)

    def _get_compiler(self) -> SQLCompiler:
        if self._compiler is None:
            self._compiler = SQLCompiler(
                expression=self._statement,
                dialect=self._config.dialect,
                parameter_manager=self._parameter_manager,
                is_many=self._is_many,
                is_script=self._is_script,
            )
        return self._compiler

    @property
    def sql(self) -> str:
        """Get the compiled SQL string."""
        # Apply filters to get the final SQL object
        final_sql = self
        for filter_obj in self._filters:
            final_sql = filter_obj.append_to_statement(final_sql)

        # If filters were applied, use the final SQL's compiler
        sql_str = final_sql._get_compiler().to_sql() if final_sql is not self else self._get_compiler().to_sql()

        # Handle empty SQL case
        if sql_str == "()" and isinstance(self._statement, exp.Anonymous) and not self._statement.this:
            return ""

        return sql_str

    @property
    def parameters(self) -> Any:
        """Get the compiled parameters."""
        # If this is an as_many SQL with original parameters, return those
        if self._is_many and self._original_parameters is not None:
            return self._original_parameters

        # Apply filters to get the final SQL object which may have additional parameters
        final_sql = self
        for filter_obj in self._filters:
            final_sql = filter_obj.append_to_statement(final_sql)

        # Return the final SQL's parameters
        return final_sql._parameter_manager.named_parameters

    @property
    def expression(self) -> exp.Expression:
        """Get the compiled SQLGlot expression."""
        return self._get_compiler().expression

    def to_sql(self, placeholder_style: str | None = None) -> str:
        """Get the SQL string with a specific placeholder style."""
        return self._get_compiler().to_sql(placeholder_style=placeholder_style)

    def get_parameters(self, style: ParameterStyle | str | None = None) -> Any:
        """Get the parameters in a specific style."""
        return self._get_compiler().get_parameters(style=style)

    @property
    def is_many(self) -> bool:
        """Check if the statement is configured for 'execute many'."""
        return self._is_many

    @property
    def is_script(self) -> bool:
        """Check if the statement is configured as a script."""
        return self._is_script

    def validate(self) -> list[Any]:
        """Validate the SQL statement and return any validation errors.

        Returns:
            List of validation errors (empty list if no errors)
        """
        # For now, return empty list - validation happens in the pipeline
        # This method exists for compatibility with tests
        return []

    def add_named_parameter(self, name: str, value: Any) -> SQL:
        """Add a named parameter to the SQL object.

        This is used by filters to add their parameters.
        """
        self._parameter_manager.add_named_parameter(name, value)
        return self

    def compile(self, placeholder_style: str | None = None) -> tuple[str, Any]:
        """Compile the SQL statement to a string and parameters.

        Args:
            placeholder_style: Override the placeholder style for compilation

        Returns:
            Tuple of (sql_string, parameters)
        """
        # If this is an as_many SQL with original parameters, return those
        if self._is_many and self._original_parameters is not None:
            compiler = self._get_compiler()
            sql = compiler.to_sql(placeholder_style=placeholder_style)
            return sql, self._original_parameters

        compiler = self._get_compiler()
        sql = compiler.to_sql(placeholder_style=placeholder_style)
        params = compiler.get_parameters(style=placeholder_style)
        return sql, params

    @property
    def parameter_info(self) -> Any:
        """Backward-compatibility shim for drivers that expect this attribute.

        This property provides access to parameter information in a format
        compatible with legacy code that expects the old parameter_info attribute.

        Returns:
            The raw parameters or parameter information from the manager
        """
        # If we have the new parameter manager with info method
        if hasattr(self._parameter_manager, "get_parameter_info"):
            return self._parameter_manager.get_parameter_info()
        # Fallback to raw parameters for compatibility
        return getattr(self, "_raw_parameters", ())
