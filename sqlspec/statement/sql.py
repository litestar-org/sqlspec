"""SQL statement handling with centralized parameter management."""

from dataclasses import dataclass, field
from typing import Any, Optional, Union

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError

from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.parameters import ParameterConverter, ParameterStyle, ParameterValidator
from sqlspec.utils.logging import get_logger

__all__ = ("SQL", "SQLConfig", "Statement")

logger = get_logger("sqlspec.statement")

Statement = Union[str, exp.Expression, "SQL"]


@dataclass
class SQLConfig:
    """Configuration for SQL statement behavior."""

    # Behavior flags
    enable_parsing: bool = True
    enable_validation: bool = True
    enable_transformations: bool = True
    enable_analysis: bool = False
    enable_normalization: bool = True
    strict_mode: bool = True
    cache_parsed_expression: bool = True

    # Component lists for explicit staging
    transformers: Optional[list[Any]] = None
    validators: Optional[list[Any]] = None
    analyzers: Optional[list[Any]] = None

    # Other configs
    parameter_converter: ParameterConverter = field(default_factory=ParameterConverter)
    parameter_validator: ParameterValidator = field(default_factory=ParameterValidator)
    analysis_cache_size: int = 1000
    input_sql_had_placeholders: bool = False  # Populated by SQL.__init__

    # Parameter style configuration
    allowed_parameter_styles: Optional[tuple[str, ...]] = None
    """Allowed parameter styles for this SQL configuration (e.g., ('qmark', 'named_colon'))."""

    target_parameter_style: Optional[str] = None
    """Target parameter style for SQL generation."""

    allow_mixed_parameter_styles: bool = False
    """Whether to allow mixing named and positional parameters in same query."""

    def validate_parameter_style(self, style: Union[ParameterStyle, str]) -> bool:
        """Check if a parameter style is allowed.

        Args:
            style: Parameter style to validate (can be ParameterStyle enum or string)

        Returns:
            True if the style is allowed, False otherwise
        """
        if self.allowed_parameter_styles is None:
            return True  # No restrictions
        style_str = str(style)
        return style_str in self.allowed_parameter_styles


class SQL:
    """Immutable SQL statement with centralized parameter management.

    The SQL class is the single source of truth for:
    - SQL expression/statement
    - Positional parameters
    - Named parameters
    - Applied filters

    All methods that modify state return new SQL instances.
    """

    __slots__ = (
        "_builder_result_type",  # Optional[type] - for query builders
        "_config",  # SQLConfig - configuration
        "_dialect",  # DialectType - SQL dialect
        "_filters",  # list[StatementFilter] - filters to apply
        "_is_many",  # bool - for executemany operations
        "_is_script",  # bool - for script execution
        "_named_params",  # dict[str, Any] - named parameters
        "_positional_params",  # list[Any] - positional parameters
        "_processed_state",  # Cached processed state
        "_statement",  # exp.Expression - the SQL expression
    )

    def __init__(
        self,
        statement: Union[str, exp.Expression, "SQL"],
        *parameters: Union[Any, StatementFilter, list[Union[Any, StatementFilter]]],
        dialect: Optional[DialectType] = None,
        config: Optional[SQLConfig] = None,
        _builder_result_type: Optional[type] = None,
        _existing_state: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQL with centralized parameter management.

        Args:
            statement: SQL string, expression, or another SQL object
            *parameters: Mixed positional args - can be parameters or filters
            dialect: SQL dialect to use
            config: SQL configuration
            _builder_result_type: Internal - for query builders
            _existing_state: Internal - for copy() method
            **kwargs: Named parameters
        """
        # Initialize config
        self._config = config or SQLConfig()
        self._dialect = dialect
        self._builder_result_type = _builder_result_type
        self._is_many = False
        self._is_script = False
        self._processed_state = None

        # Initialize parameter storage
        self._positional_params: list[Any] = []
        self._named_params: dict[str, Any] = {}
        self._filters: list[StatementFilter] = []
        self._statement: exp.Expression  # Will be set below

        # Handle statement conversion
        if isinstance(statement, SQL):
            # Copy from existing SQL object
            self._statement = statement._statement
            self._dialect = dialect or statement._dialect
            self._config = config or statement._config
            self._builder_result_type = _builder_result_type or statement._builder_result_type
            self._is_many = statement._is_many
            self._is_script = statement._is_script
            # Copy internal state
            self._positional_params.extend(statement._positional_params)
            self._named_params.update(statement._named_params)
            self._filters.extend(statement._filters)
        else:
            # Convert to expression if string
            self._statement = self._to_expression(statement) if isinstance(statement, str) else statement

        # Load from existing state if provided (used by copy())
        if _existing_state:
            self._positional_params = list(_existing_state.get("positional_params", []))
            self._named_params = dict(_existing_state.get("named_params", {}))
            self._filters = list(_existing_state.get("filters", []))
            if "is_many" in _existing_state:
                self._is_many = _existing_state["is_many"]
            if "is_script" in _existing_state:
                self._is_script = _existing_state["is_script"]

        # Process parameters from *args
        for param in parameters:
            if isinstance(param, StatementFilter):
                # Store filter and extract its parameters
                self._filters.append(param)
                pos_params, named_params = self._extract_filter_parameters(param)
                self._positional_params.extend(pos_params)
                self._named_params.update(named_params)
            elif isinstance(param, list):
                # Handle list of items
                for item in param:
                    if isinstance(item, StatementFilter):
                        self._filters.append(item)
                        pos_params, named_params = self._extract_filter_parameters(item)
                        self._positional_params.extend(pos_params)
                        self._named_params.update(named_params)
                    elif isinstance(item, dict):
                        # Merge dict as named params
                        self._named_params.update(item)
                    else:
                        # Add as positional param
                        self._positional_params.append(item)
            elif isinstance(param, dict):
                # Merge dict as named params
                self._named_params.update(param)
            else:
                # Add as positional param
                self._positional_params.append(param)

        # Process **kwargs - highest precedence
        self._named_params.update(kwargs)

    def _to_expression(self, statement: Union[str, exp.Expression]) -> exp.Expression:
        """Convert string to sqlglot expression."""
        if isinstance(statement, exp.Expression):
            return statement

        try:
            # Parse with sqlglot
            expressions = sqlglot.parse(statement, dialect=self._dialect)
            if not expressions:
                # Empty statement
                return exp.Select()
            first_expr = expressions[0]
            if first_expr is None:
                # Could not parse
                return exp.Anonymous(this=statement)
            return first_expr
        except ParseError as e:
            # If parsing fails, wrap in a RawString expression
            logger.debug("Failed to parse SQL: %s", e)
            return exp.Anonymous(this=statement)

    def _extract_filter_parameters(self, filter_obj: StatementFilter) -> tuple[list[Any], dict[str, Any]]:
        """Extract parameters from a filter object."""
        if hasattr(filter_obj, "extract_parameters"):
            return filter_obj.extract_parameters()
        # Fallback for filters that don't implement the new method yet
        return [], {}

    def copy(
        self,
        statement: Optional[Union[str, exp.Expression]] = None,
        parameters: Optional[Any] = None,
        dialect: Optional[DialectType] = None,
        config: Optional[SQLConfig] = None,
        **kwargs: Any,
    ) -> "SQL":
        """Create a copy with optional modifications.

        This is the primary method for creating modified SQL objects.
        """
        # Prepare existing state
        existing_state = {
            "positional_params": list(self._positional_params),
            "named_params": dict(self._named_params),
            "filters": list(self._filters),
            "is_many": self._is_many,
            "is_script": self._is_script,
        }

        # Create new instance
        new_statement = statement if statement is not None else self._statement
        new_dialect = dialect if dialect is not None else self._dialect
        new_config = config if config is not None else self._config

        # If parameters are explicitly provided, they replace existing ones
        if parameters is not None:
            # Clear existing state so only new parameters are used
            existing_state["positional_params"] = []
            existing_state["named_params"] = {}
            # Pass parameters through normal processing
            return SQL(
                new_statement,
                parameters,
                dialect=new_dialect,
                config=new_config,
                _builder_result_type=self._builder_result_type,
                _existing_state=None,  # Don't use existing state
                **kwargs,
            )

        return SQL(
            new_statement,
            dialect=new_dialect,
            config=new_config,
            _builder_result_type=self._builder_result_type,
            _existing_state=existing_state,
            **kwargs,
        )

    def add_named_parameter(self, name: str, value: Any) -> "SQL":
        """Add a named parameter and return a new SQL instance."""
        new_obj = self.copy()
        new_obj._named_params[name] = value
        return new_obj

    def get_unique_parameter_name(self, base_name: str) -> str:
        """Generate a unique parameter name."""
        # Check both positional and named params
        all_param_names = set(self._named_params.keys())

        # If base_name is unique, use it
        if base_name not in all_param_names:
            return base_name

        # Generate unique name
        counter = 0
        while True:
            candidate = f"{base_name}_{counter}"
            if candidate not in all_param_names:
                return candidate
            counter += 1

    def where(self, condition: "Union[str, exp.Expression, exp.Condition]") -> "SQL":
        """Apply WHERE clause and return new SQL instance."""
        # Convert condition to expression
        condition_expr = self._to_expression(condition) if isinstance(condition, str) else condition

        # Apply WHERE to statement
        if hasattr(self._statement, "where"):
            new_statement = self._statement.where(condition_expr)
        else:
            # Wrap in SELECT if needed
            new_statement = exp.Select().from_(self._statement).where(condition_expr)

        return self.copy(statement=new_statement)

    def as_many(self, parameters: "Optional[list[Any]]" = None) -> "SQL":
        """Mark for executemany with optional parameters."""
        new_obj = self.copy()
        new_obj._is_many = True
        if parameters is not None:
            # Replace parameters for executemany
            new_obj._positional_params = []
            new_obj._named_params = {}
            if isinstance(parameters, list):
                new_obj._positional_params = parameters
        return new_obj

    def as_script(self) -> "SQL":
        """Mark as script for execution."""
        new_obj = self.copy()
        new_obj._is_script = True
        return new_obj

    def _build_final_state(self) -> tuple[exp.Expression, Any]:
        """Build final expression and parameters after applying filters."""
        # Start with current statement
        final_expr = self._statement

        # Apply all filters to the expression
        for filter_obj in self._filters:
            if hasattr(filter_obj, "append_to_statement"):
                # New style filter that modifies expression directly
                temp_sql = SQL(final_expr, config=self._config, dialect=self._dialect)
                temp_sql._positional_params = list(self._positional_params)
                temp_sql._named_params = dict(self._named_params)
                result = filter_obj.append_to_statement(temp_sql)
                final_expr = result._statement if isinstance(result, SQL) else result
            elif hasattr(filter_obj, "apply"):
                # Legacy filter - needs to be updated
                # For now, create a temporary SQL object to apply it
                temp_sql = SQL(final_expr, config=self._config, dialect=self._dialect)
                temp_sql._positional_params = list(self._positional_params)
                temp_sql._named_params = dict(self._named_params)
                result = filter_obj.apply(temp_sql)
                if isinstance(result, SQL):
                    final_expr = result._statement

        # Determine final parameters format
        final_params: Any
        if self._named_params and not self._positional_params:
            # Only named params
            final_params = dict(self._named_params)
        elif self._positional_params and not self._named_params:
            # Only positional params
            if len(self._positional_params) == 1 and not self._is_many:
                # Single positional param
                final_params = self._positional_params[0]
            else:
                # Multiple positional params
                final_params = list(self._positional_params)
        elif self._positional_params and self._named_params:
            # Mixed - merge into dict
            final_params = dict(self._named_params)
            # Add positional params with generated names
            for i, param in enumerate(self._positional_params):
                param_name = f"param_{i}"
                while param_name in final_params:
                    param_name = f"param_{i}_{id(param)}"
                final_params[param_name] = param
        else:
            # No parameters
            final_params = None

        return final_expr, final_params

    # Properties for compatibility
    @property
    def sql(self) -> str:
        """Get SQL string."""
        expr, _ = self._build_final_state()
        return expr.sql(dialect=self._dialect) if hasattr(expr, "sql") else str(expr)

    @property
    def expression(self) -> exp.Expression:
        """Get the final expression."""
        expr, _ = self._build_final_state()
        return expr

    @property
    def parameters(self) -> Any:
        """Get merged parameters."""
        _, params = self._build_final_state()
        return params

    @property
    def is_many(self) -> bool:
        """Check if this is for executemany."""
        return self._is_many

    @property
    def is_script(self) -> bool:
        """Check if this is a script."""
        return self._is_script

    def to_sql(self, placeholder_style: Optional[str] = None) -> str:
        """Convert to SQL string with given placeholder style."""
        expr, _ = self._build_final_state()
        # TODO: Implement placeholder style conversion
        return expr.sql(dialect=self._dialect) if hasattr(expr, "sql") else str(expr)

    def get_parameters(self, style: Optional[str] = None) -> Any:
        """Get parameters in the requested style."""
        _, params = self._build_final_state()
        # TODO: Implement parameter style conversion
        return params

    def compile(self, placeholder_style: Optional[str] = None) -> tuple[str, Any]:
        """Compile to SQL and parameters."""
        return self.to_sql(placeholder_style), self.get_parameters(style=placeholder_style)

    # Validation properties for compatibility
    @property
    def validation_errors(self) -> list[Any]:
        """Get validation errors."""
        # TODO: Implement validation
        return []

    @property
    def has_errors(self) -> bool:
        """Check if there are validation errors."""
        return bool(self.validation_errors)

    @property
    def is_safe(self) -> bool:
        """Check if statement is safe."""
        return not self.has_errors

    # Additional compatibility methods
    @property
    def _raw_parameters(self) -> Any:
        """Get raw parameters for compatibility."""
        return self.parameters

    @_raw_parameters.setter
    def _raw_parameters(self, value: Any) -> None:
        """Set raw parameters for compatibility."""
        # Clear existing params
        self._positional_params = []
        self._named_params = {}
        # Add new params
        if isinstance(value, dict):
            self._named_params.update(value)
        elif isinstance(value, (list, tuple)):
            self._positional_params = list(value)
        elif value is not None:
            self._positional_params = [value]

    @property
    def _sql(self) -> str:
        """Get SQL string for compatibility."""
        return self.sql

    @property
    def _expression(self) -> exp.Expression:
        """Get expression for compatibility."""
        return self.expression

    @property
    def statement(self) -> exp.Expression:
        """Get statement for compatibility."""
        return self._statement

    def limit(self, count: int, use_parameter: bool = False) -> "SQL":
        """Add LIMIT clause."""
        if hasattr(self._statement, "limit"):
            new_statement = self._statement.limit(count)
        else:
            new_statement = exp.Select().from_(self._statement).limit(count)
        return self.copy(statement=new_statement)

    def offset(self, count: int, use_parameter: bool = False) -> "SQL":
        """Add OFFSET clause."""
        if hasattr(self._statement, "offset"):
            new_statement = self._statement.offset(count)
        else:
            new_statement = exp.Select().from_(self._statement).offset(count)
        return self.copy(statement=new_statement)

    def order_by(self, expression: exp.Expression) -> "SQL":
        """Add ORDER BY clause."""
        if hasattr(self._statement, "order_by"):
            new_statement = self._statement.order_by(expression)
        else:
            new_statement = exp.Select().from_(self._statement).order_by(expression)
        return self.copy(statement=new_statement)
