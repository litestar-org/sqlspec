"""SQL statement handling with centralized parameter management."""

import operator
from dataclasses import dataclass, field
from typing import Any, Optional, Union

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.dialect import DialectType
from sqlglot.errors import ParseError

from sqlspec.exceptions import RiskLevel, SQLValidationError
from sqlspec.statement.filters import StatementFilter
from sqlspec.statement.parameters import ParameterConverter, ParameterStyle, ParameterValidator
from sqlspec.statement.pipelines.base import StatementPipeline
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.utils.logging import get_logger

__all__ = ("SQL", "SQLConfig", "Statement")

logger = get_logger("sqlspec.statement")

Statement = Union[str, exp.Expression, "SQL"]


@dataclass
class _ProcessedState:
    """Cached state from pipeline processing."""

    processed_expression: exp.Expression
    processed_sql: str
    merged_parameters: Any
    validation_errors: list[Any] = field(default_factory=list)
    analysis_results: dict[str, Any] = field(default_factory=dict)
    transformation_results: dict[str, Any] = field(default_factory=dict)


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

    def get_statement_pipeline(self) -> StatementPipeline:
        """Get the configured statement pipeline.

        Returns:
            StatementPipeline configured with transformers, validators, and analyzers
        """
        # Import here to avoid circular dependencies
        from sqlspec.statement.pipelines.transformers import CommentRemover, ExpressionSimplifier, ParameterizeLiterals
        from sqlspec.statement.pipelines.validators import DMLSafetyValidator, ParameterStyleValidator

        # Create transformers based on config
        transformers = []
        if self.transformers is not None:
            # Use explicit transformers if provided
            transformers = list(self.transformers)
        # Use default transformers
        elif self.enable_transformations:
            transformers = [CommentRemover(), ParameterizeLiterals(), ExpressionSimplifier()]

        # Create validators based on config
        validators = []
        if self.validators is not None:
            # Use explicit validators if provided
            validators = list(self.validators)
        # Use default validators
        elif self.enable_validation:
            validators = [
                ParameterStyleValidator(),
                DMLSafetyValidator(),
                # PerformanceValidator(),  # Commented out to allow SELECT * in tests
            ]

        # Create analyzers based on config
        analyzers = []
        if self.analyzers is not None:
            # Use explicit analyzers if provided
            analyzers = list(self.analyzers)
        # Use default analyzers
        elif self.enable_analysis:
            # Currently no default analyzers
            analyzers = []

        return StatementPipeline(transformers=transformers, validators=validators, analyzers=analyzers)


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
        "_raw_sql",  # str - original SQL string for compatibility
        "_statement",  # exp.Expression - the SQL expression
    )

    def __init__(
        self,
        statement: Union[str, exp.Expression, "SQL"],
        *parameters: Union[Any, StatementFilter, list[Union[Any, StatementFilter]]],
        _dialect: Optional[DialectType] = None,
        _config: Optional[SQLConfig] = None,
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
        self._config = _config or SQLConfig()
        self._dialect = _dialect
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
            self._dialect = _dialect or statement._dialect
            self._config = _config or statement._config
            self._builder_result_type = _builder_result_type or statement._builder_result_type
            self._is_many = statement._is_many
            self._is_script = statement._is_script
            self._raw_sql: str = statement._raw_sql
            # Copy internal state
            self._positional_params.extend(statement._positional_params)
            self._named_params.update(statement._named_params)
            self._filters.extend(statement._filters)
        else:
            # Store raw SQL string if provided
            if isinstance(statement, str):
                self._raw_sql = statement
            elif isinstance(statement, exp.Expression):
                self._raw_sql = statement.sql(dialect=self._dialect)
            else:
                self._raw_sql = ""
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
            if "raw_sql" in _existing_state:
                self._raw_sql = _existing_state["raw_sql"]

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
        # Special handling for 'parameters' keyword
        if "parameters" in kwargs:
            param_value = kwargs.pop("parameters")
            if isinstance(param_value, (list, tuple)):
                # Add as positional parameters
                self._positional_params.extend(param_value)
            elif isinstance(param_value, dict):
                # Merge as named parameters
                self._named_params.update(param_value)
            else:
                # Single value
                self._positional_params.append(param_value)

        # Add remaining kwargs as named parameters
        # Skip internal parameters that start with underscore
        for key, value in kwargs.items():
            if not key.startswith("_"):
                self._named_params[key] = value

    def _ensure_processed(self) -> None:
        """Ensure the SQL has been processed through the pipeline (lazy initialization).

        This method implements the facade pattern with lazy processing.
        It's called by public methods that need processed state.
        """
        if self._processed_state is not None:
            return

        # Get the final expression and parameters after filters
        final_expr, final_params = self._build_final_state()

        # Create processing context
        context = SQLProcessingContext(
            initial_sql_string=self._raw_sql or final_expr.sql(dialect=self._dialect),
            dialect=self._dialect,
            config=self._config,
            current_expression=final_expr,
            initial_expression=final_expr,
            merged_parameters=final_params,
            input_sql_had_placeholders=self._config.input_sql_had_placeholders,
        )

        # Extract parameter info from the SQL
        validator = self._config.parameter_validator
        context.parameter_info = validator.extract_parameters(context.initial_sql_string)

        # Run the pipeline
        pipeline = self._config.get_statement_pipeline()
        result = pipeline.execute_pipeline(context)

        # Extract processed state
        processed_expr = result.expression
        processed_sql = processed_expr.sql(dialect=self._dialect, comments=False)

        # Merge parameters from pipeline
        merged_params = final_params
        if result.context.extracted_parameters_from_pipeline:
            if isinstance(merged_params, dict):
                for i, param in enumerate(result.context.extracted_parameters_from_pipeline):
                    param_name = f"_arg_{i}"
                    merged_params[param_name] = param
            elif isinstance(merged_params, list):
                merged_params.extend(result.context.extracted_parameters_from_pipeline)
            elif merged_params is None:
                merged_params = result.context.extracted_parameters_from_pipeline
            else:
                # Single value, convert to list
                merged_params = [merged_params, *list(result.context.extracted_parameters_from_pipeline)]

        # Cache the processed state
        self._processed_state = _ProcessedState(
            processed_expression=processed_expr,
            processed_sql=processed_sql,
            merged_parameters=merged_params,
            validation_errors=list(result.context.validation_errors),
            analysis_results={},  # Can be populated from analysis_findings if needed
            transformation_results={},  # Can be populated from transformations if needed
        )

        # Check strict mode
        if self._config.strict_mode and self._processed_state.validation_errors:
            # Find the highest risk error
            highest_risk_error = max(
                self._processed_state.validation_errors,
                key=lambda e: e.risk_level.value if hasattr(e, "risk_level") else 0,
            )
            raise SQLValidationError(
                message=highest_risk_error.message,
                sql=self._raw_sql or processed_sql,
                risk_level=getattr(highest_risk_error, "risk_level", RiskLevel.HIGH),
            )

    def _to_expression(self, statement: Union[str, exp.Expression]) -> exp.Expression:
        """Convert string to sqlglot expression."""
        if isinstance(statement, exp.Expression):
            return statement

        # Handle empty string
        if not statement or not statement.strip():
            # Return an empty select instead of Anonymous for empty strings
            return exp.Select()

        # Check if parsing is disabled
        if not self._config.enable_parsing:
            # Return an anonymous expression that preserves the raw SQL
            return exp.Anonymous(this=statement)

        try:
            # Parse with sqlglot
            expressions = sqlglot.parse(statement, dialect=self._dialect)
            if not expressions:
                # Empty statement
                return exp.Anonymous(this=statement)
            first_expr = expressions[0]
            if first_expr is None:
                # Could not parse
                return exp.Anonymous(this=statement)

        except ParseError as e:
            # If parsing fails, wrap in a RawString expression
            logger.debug("Failed to parse SQL: %s", e)
            return exp.Anonymous(this=statement)
        return first_expr

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
            "raw_sql": self._raw_sql,
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

    def get_unique_parameter_name(
        self, base_name: str, namespace: Optional[str] = None, preserve_original: bool = False
    ) -> str:
        """Generate a unique parameter name.

        Args:
            base_name: The base parameter name
            namespace: Optional namespace prefix (e.g., 'cte', 'subquery')
            preserve_original: If True, try to preserve the original name

        Returns:
            A unique parameter name
        """
        # Check both positional and named params
        all_param_names = set(self._named_params.keys())

        # Build the candidate name
        candidate = f"{namespace}_{base_name}" if namespace else base_name

        # If preserve_original and the name is unique, use it
        if preserve_original and candidate not in all_param_names:
            return candidate

        # If not preserving or name exists, generate unique name
        if candidate not in all_param_names:
            return candidate

        # Generate unique name with counter
        counter = 1
        while True:
            new_candidate = f"{candidate}_{counter}"
            if new_candidate not in all_param_names:
                return new_candidate
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

    def filter(self, filter_obj: StatementFilter) -> "SQL":
        """Apply a filter and return a new SQL instance."""
        # Create a new SQL object with the filter added
        new_obj = self.copy()
        new_obj._filters.append(filter_obj)
        # Extract filter parameters
        pos_params, named_params = self._extract_filter_parameters(filter_obj)
        new_obj._positional_params.extend(pos_params)
        new_obj._named_params.update(named_params)
        return new_obj

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
                param_name = f"_arg_{i}"
                while param_name in final_params:
                    param_name = f"_arg_{i}_{id(param)}"
                final_params[param_name] = param
        else:
            # No parameters
            final_params = None

        return final_expr, final_params

    # Properties for compatibility
    @property
    def sql(self) -> str:
        """Get SQL string."""
        # Handle empty string case
        if self._raw_sql == "" or (self._raw_sql and not self._raw_sql.strip()):
            return ""

        # For scripts, always return the raw SQL to preserve multi-statement scripts
        if self._is_script and self._raw_sql:
            return self._raw_sql
        # If parsing is disabled, return the raw SQL
        if not self._config.enable_parsing and self._raw_sql:
            return self._raw_sql

        # Ensure processed
        self._ensure_processed()
        return self._processed_state.processed_sql

    @property
    def expression(self) -> Optional[exp.Expression]:
        """Get the final expression."""
        # Return None if parsing is disabled
        if not self._config.enable_parsing:
            return None
        self._ensure_processed()
        return self._processed_state.processed_expression

    @property
    def parameters(self) -> Any:
        """Get merged parameters."""
        self._ensure_processed()
        return self._processed_state.merged_parameters

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
        if self._is_script:
            return self.sql
        sql, _ = self.compile(placeholder_style=placeholder_style)
        return sql

    def get_parameters(self, style: Optional[str] = None) -> Any:
        """Get parameters in the requested style."""
        # Get compiled parameters with style
        _, params = self.compile(placeholder_style=style)
        return params

    def compile(self, placeholder_style: Optional[str] = None) -> tuple[str, Any]:
        """Compile to SQL and parameters."""
        # Ensure processed
        self._ensure_processed()

        # Get processed SQL and parameters
        sql = self._processed_state.processed_sql
        params = self._processed_state.merged_parameters

        # If no placeholder style requested, return as-is
        if placeholder_style is None:
            return sql, params

        # Convert to requested placeholder style
        if placeholder_style and params is not None:
            # Extract parameter info from current SQL
            converter = self._config.parameter_converter
            param_info = converter.validator.extract_parameters(sql)

            # Convert SQL to target style
            if param_info:
                # Use the internal denormalize method to convert to target style
                from sqlspec.statement.parameters import ParameterStyle

                target_style = (
                    ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
                )
                # Replace placeholders with target style
                # Sort by position in reverse to avoid position shifts
                sorted_params = sorted(param_info, key=lambda p: p.position, reverse=True)
                for p in sorted_params:
                    # Generate new placeholder based on target style
                    if target_style == ParameterStyle.QMARK:
                        new_placeholder = "?"
                    elif target_style == ParameterStyle.NUMERIC:
                        # Use 1-based numbering for numeric style
                        new_placeholder = f"${p.ordinal + 1}"
                    elif target_style == ParameterStyle.NAMED_COLON:
                        # Use generated parameter names
                        new_placeholder = f":param_{p.ordinal}"
                    elif target_style == ParameterStyle.POSITIONAL_COLON:
                        # Keep the original numeric placeholder
                        new_placeholder = p.placeholder_text
                    else:
                        # Keep original for unknown styles
                        new_placeholder = p.placeholder_text

                    # Replace the placeholder in SQL
                    start = p.position
                    end = start + len(p.placeholder_text)
                    sql = sql[:start] + new_placeholder + sql[end:]

                # Convert parameters to appropriate format for target style
                if target_style == ParameterStyle.POSITIONAL_COLON:
                    # Convert to dict format for Oracle numeric style
                    if isinstance(params, (list, tuple)):
                        # For Oracle numeric parameters, map based on numeric order
                        # First, extract only Oracle numeric parameters and sort by numeric value
                        oracle_params = [
                            (int(p.name), p)
                            for p in param_info
                            if p.style == ParameterStyle.POSITIONAL_COLON and p.name and p.name.isdigit()
                        ]
                        oracle_params.sort(key=operator.itemgetter(0))  # Sort by numeric value

                        # Map list items to parameters based on numeric order
                        result_dict = {}
                        for i, (_, p) in enumerate(oracle_params):
                            if i < len(params):
                                result_dict[p.name] = params[i]
                        params = result_dict
                    elif not isinstance(params, dict):
                        # Single value - map to first parameter name
                        if param_info:
                            params = {param_info[0].name: params}
                elif target_style in {ParameterStyle.QMARK, ParameterStyle.NUMERIC} and isinstance(params, dict):
                    params = [params.get(p.name, None) for p in param_info if p.name in params]

        return sql, params

    # Validation properties for compatibility
    @property
    def validation_errors(self) -> list[Any]:
        """Get validation errors."""
        if not self._config.enable_validation:
            return []
        self._ensure_processed()
        return self._processed_state.validation_errors

    @property
    def has_errors(self) -> bool:
        """Check if there are validation errors."""
        return bool(self.validation_errors)

    @property
    def is_safe(self) -> bool:
        """Check if statement is safe."""
        return not self.has_errors

    # Additional compatibility methods
    def validate(self) -> list[Any]:
        """Validate the SQL statement and return validation errors."""
        return self.validation_errors

    @property
    def parameter_info(self) -> list[Any]:
        """Get parameter information from the SQL statement."""
        # Use the parameter validator to extract parameter info
        validator = self._config.parameter_validator
        return validator.extract_parameters(self.sql)

    @property
    def _raw_parameters(self) -> Any:
        """Get raw parameters for compatibility."""
        # Return raw parameters without processing
        _, params = self._build_final_state()
        # For backward compatibility, return None instead of empty list
        if isinstance(params, list) and len(params) == 0:
            return None
        return params

    @property
    def _sql(self) -> str:
        """Get SQL string for compatibility."""
        return self.sql

    @property
    def _expression(self) -> Optional[exp.Expression]:
        """Get expression for compatibility."""
        return self.expression

    @property
    def statement(self) -> exp.Expression:
        """Get statement for compatibility."""
        return self._statement

    def limit(self, count: int, use_parameter: bool = False) -> "SQL":
        """Add LIMIT clause."""
        if use_parameter:
            # Create a unique parameter name
            param_name = self.get_unique_parameter_name("limit")
            # Add parameter to the SQL object
            result = self
            result = result.add_named_parameter(param_name, count)
            # Use placeholder in the expression
            if hasattr(result._statement, "limit"):
                new_statement = result._statement.limit(exp.Placeholder(this=param_name))
            else:
                new_statement = exp.Select().from_(result._statement).limit(exp.Placeholder(this=param_name))
            return result.copy(statement=new_statement)
        if hasattr(self._statement, "limit"):
            new_statement = self._statement.limit(count)
        else:
            new_statement = exp.Select().from_(self._statement).limit(count)
        return self.copy(statement=new_statement)

    def offset(self, count: int, use_parameter: bool = False) -> "SQL":
        """Add OFFSET clause."""
        if use_parameter:
            # Create a unique parameter name
            param_name = self.get_unique_parameter_name("offset")
            # Add parameter to the SQL object
            result = self
            result = result.add_named_parameter(param_name, count)
            # Use placeholder in the expression
            if hasattr(result._statement, "offset"):
                new_statement = result._statement.offset(exp.Placeholder(this=param_name))
            else:
                new_statement = exp.Select().from_(result._statement).offset(exp.Placeholder(this=param_name))
            return result.copy(statement=new_statement)
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
