"""SQL statement handling with centralized parameter management."""

from dataclasses import dataclass, field, replace
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
from sqlspec.statement.pipelines.transformers import CommentRemover, ParameterizeLiterals
from sqlspec.statement.pipelines.validators import DMLSafetyValidator, ParameterStyleValidator
from sqlspec.typing import is_dict
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
    strict_mode: bool = False
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

        # Create transformers based on config
        transformers = []
        if self.transformers is not None:
            # Use explicit transformers if provided
            transformers = list(self.transformers)
        # Use default transformers
        elif self.enable_transformations:
            # Use target_parameter_style if available, otherwise default to "?"
            placeholder_style = self.target_parameter_style or "?"
            transformers = [CommentRemover(), ParameterizeLiterals(placeholder_style=placeholder_style)]

        # Create validators based on config
        validators = []
        if self.validators is not None:
            # Use explicit validators if provided
            validators = list(self.validators)
        # Use default validators
        elif self.enable_validation:
            validators = [ParameterStyleValidator(fail_on_violation=self.strict_mode), DMLSafetyValidator()]

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
        "_original_parameters",  # Any - original parameters as passed in
        "_original_sql",  # str - original SQL before normalization
        "_placeholder_mapping",  # dict[str, Union[str, int]] - placeholder normalization mapping
        "_positional_params",  # list[Any] - positional parameters
        "_processed_state",  # Cached processed state
        "_processing_context",  # SQLProcessingContext - context from pipeline processing
        "_raw_sql",  # str - original SQL string for compatibility
        "_statement",  # exp.Expression - the SQL expression
    )

    def __init__(
        self,
        statement: Union[str, exp.Expression, "SQL"],
        *parameters: Union[Any, StatementFilter, list[Union[Any, StatementFilter]]],
        _dialect: DialectType = None,
        _config: Optional[SQLConfig] = None,
        _builder_result_type: Optional[type] = None,
        _existing_state: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize SQL with centralized parameter management."""
        self._config = _config or SQLConfig()
        self._dialect = _dialect
        self._builder_result_type = _builder_result_type
        self._processed_state: Optional[_ProcessedState] = None
        self._processing_context: Optional[SQLProcessingContext] = None
        self._positional_params: list[Any] = []
        self._named_params: dict[str, Any] = {}
        self._filters: list[StatementFilter] = []
        self._statement: exp.Expression
        self._raw_sql: str = ""
        self._original_parameters: Any = None
        self._original_sql: str = ""
        self._placeholder_mapping: dict[str, Union[str, int]] = {}
        self._is_many: bool = False
        self._is_script: bool = False

        if isinstance(statement, SQL):
            self._init_from_sql_object(statement, _dialect, _config, _builder_result_type)
        else:
            self._init_from_str_or_expression(statement)

        if _existing_state:
            self._load_from_existing_state(_existing_state)

        if not isinstance(statement, SQL):
            self._set_original_parameters(*parameters)

        self._process_parameters(*parameters, **kwargs)

    def _init_from_sql_object(
        self, statement: "SQL", dialect: DialectType, config: Optional[SQLConfig], builder_result_type: Optional[type]
    ) -> None:
        """Initialize attributes from an existing SQL object."""
        self._statement = statement._statement
        self._dialect = dialect or statement._dialect
        self._config = config or statement._config
        self._builder_result_type = builder_result_type or statement._builder_result_type
        self._is_many = statement._is_many
        self._is_script = statement._is_script
        self._raw_sql = statement._raw_sql
        self._original_parameters = statement._original_parameters
        self._original_sql = statement._original_sql
        self._placeholder_mapping = statement._placeholder_mapping.copy()
        self._positional_params.extend(statement._positional_params)
        self._named_params.update(statement._named_params)
        self._filters.extend(statement._filters)

    def _init_from_str_or_expression(self, statement: Union[str, exp.Expression]) -> None:
        """Initialize attributes from a SQL string or expression."""
        if isinstance(statement, str):
            self._raw_sql = statement
            if self._raw_sql and not self._config.input_sql_had_placeholders:
                param_info = self._config.parameter_validator.extract_parameters(self._raw_sql)
                if param_info:
                    self._config = replace(self._config, input_sql_had_placeholders=True)
            self._statement = self._to_expression(statement)
        else:
            self._raw_sql = statement.sql(dialect=self._dialect)  # pyright: ignore
            self._statement = statement

    def _load_from_existing_state(self, existing_state: dict[str, Any]) -> None:
        """Load state from a dictionary (used by copy)."""
        self._positional_params = list(existing_state.get("positional_params", self._positional_params))
        self._named_params = dict(existing_state.get("named_params", self._named_params))
        self._filters = list(existing_state.get("filters", self._filters))
        self._is_many = existing_state.get("is_many", self._is_many)
        self._is_script = existing_state.get("is_script", self._is_script)
        self._raw_sql = existing_state.get("raw_sql", self._raw_sql)

    def _set_original_parameters(self, *parameters: Any) -> None:
        """Store the original parameters for compatibility."""
        if len(parameters) == 1 and not isinstance(parameters[0], StatementFilter):
            self._original_parameters = parameters[0]
        elif len(parameters) > 1:
            self._original_parameters = parameters
        else:
            self._original_parameters = None

    def _process_parameters(self, *parameters: Any, **kwargs: Any) -> None:
        """Process positional and keyword arguments for parameters and filters."""
        for param in parameters:
            self._process_parameter_item(param)

        if "parameters" in kwargs:
            param_value = kwargs.pop("parameters")
            if isinstance(param_value, (list, tuple)):
                self._positional_params.extend(param_value)
            elif isinstance(param_value, dict):
                self._named_params.update(param_value)
            else:
                self._positional_params.append(param_value)

        for key, value in kwargs.items():
            if not key.startswith("_"):
                self._named_params[key] = value

    def _process_parameter_item(self, item: Any) -> None:
        """Process a single item from the parameters list."""
        if isinstance(item, StatementFilter):
            self._filters.append(item)
            pos_params, named_params = self._extract_filter_parameters(item)
            self._positional_params.extend(pos_params)
            self._named_params.update(named_params)
        elif isinstance(item, list):
            for sub_item in item:
                self._process_parameter_item(sub_item)
        elif isinstance(item, dict):
            self._named_params.update(item)
        elif isinstance(item, tuple):
            self._positional_params.extend(item)
        else:
            self._positional_params.append(item)

    def _ensure_processed(self) -> None:
        """Ensure the SQL has been processed through the pipeline (lazy initialization).

        This method implements the facade pattern with lazy processing.
        It's called by public methods that need processed state.
        """
        if self._processed_state is not None:
            return

        # Get the final expression and parameters after filters
        final_expr, final_params = self._build_final_state()

        # Check if the raw SQL has placeholders
        if self._raw_sql:
            validator = self._config.parameter_validator
            raw_param_info = validator.extract_parameters(self._raw_sql)
            has_placeholders = bool(raw_param_info)
        else:
            has_placeholders = self._config.input_sql_had_placeholders

        # Update config if we detected placeholders
        if has_placeholders and not self._config.input_sql_had_placeholders:
            self._config = replace(self._config, input_sql_had_placeholders=True)

        # Create processing context
        context = SQLProcessingContext(
            initial_sql_string=self._raw_sql or final_expr.sql(dialect=self._dialect),
            dialect=self._dialect,
            config=self._config,
            current_expression=final_expr,
            initial_expression=final_expr,
            merged_parameters=final_params,
            input_sql_had_placeholders=has_placeholders,
        )

        # Extract parameter info from the SQL
        validator = self._config.parameter_validator
        context.parameter_info = validator.extract_parameters(context.initial_sql_string)

        # Run the pipeline
        pipeline = self._config.get_statement_pipeline()
        result = pipeline.execute_pipeline(context)

        # Store the processing context for later use
        self._processing_context = result.context

        # Extract processed state
        processed_expr = result.expression
        if isinstance(processed_expr, exp.Anonymous):
            processed_sql = self._raw_sql or context.initial_sql_string
        else:
            processed_sql = processed_expr.sql(dialect=self._dialect, comments=False)
            logger.debug("Processed expression SQL: '%s'", processed_sql)

            # Check if we need to denormalize pyformat placeholders
            if self._placeholder_mapping and self._original_sql:
                # We normalized pyformat placeholders before parsing, need to denormalize
                original_sql = self._original_sql
                # Extract parameter info from the original SQL to get the original styles
                param_info = self._config.parameter_validator.extract_parameters(original_sql)

                # Find the target style (should be pyformat)
                from sqlspec.statement.parameters import ParameterStyle

                target_styles = {p.style for p in param_info}
                logger.debug(
                    "Denormalizing SQL: before='%s', original='%s', styles=%s",
                    processed_sql,
                    original_sql,
                    target_styles,
                )
                if ParameterStyle.POSITIONAL_PYFORMAT in target_styles:
                    # Denormalize back to %s
                    processed_sql = self._config.parameter_converter._denormalize_sql(
                        processed_sql, param_info, ParameterStyle.POSITIONAL_PYFORMAT
                    )
                    logger.debug("Denormalized SQL to: '%s'", processed_sql)
                elif ParameterStyle.NAMED_PYFORMAT in target_styles:
                    # Denormalize back to %(name)s
                    processed_sql = self._config.parameter_converter._denormalize_sql(
                        processed_sql, param_info, ParameterStyle.NAMED_PYFORMAT
                    )
                    logger.debug("Denormalized SQL to: '%s'", processed_sql)
            else:
                logger.debug(
                    "No denormalization needed: mapping=%s, original=%s",
                    bool(self._placeholder_mapping),
                    bool(self._original_sql),
                )

        # Merge parameters from pipeline
        merged_params = final_params
        # Only merge extracted parameters if the original SQL didn't have placeholders
        # If it already had placeholders, the parameters should already be provided
        if result.context.extracted_parameters_from_pipeline and not context.input_sql_had_placeholders:
            if isinstance(merged_params, dict):
                for i, param in enumerate(result.context.extracted_parameters_from_pipeline):
                    param_name = f"param_{i}"
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

        # Check if SQL contains pyformat placeholders that need normalization
        from sqlspec.statement.parameters import ParameterStyle

        validator = self._config.parameter_validator
        param_info = validator.extract_parameters(statement)

        # Check if we have pyformat placeholders
        has_pyformat = any(
            p.style in {ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT} for p in param_info
        )

        normalized_sql = statement
        placeholder_mapping: dict[str, Any] = {}

        if has_pyformat:
            # Normalize pyformat placeholders to named placeholders for SQLGlot
            converter = self._config.parameter_converter
            normalized_sql, placeholder_mapping = converter._transform_sql_for_parsing(statement, param_info)
            # Store the original SQL before normalization
            self._original_sql = statement
            self._placeholder_mapping = placeholder_mapping

        try:
            # Parse with sqlglot
            expressions = sqlglot.parse(normalized_sql, dialect=self._dialect)  # pyright: ignore
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

    @staticmethod
    def _extract_filter_parameters(filter_obj: StatementFilter) -> tuple[list[Any], dict[str, Any]]:
        """Extract parameters from a filter object."""
        if hasattr(filter_obj, "extract_parameters"):
            return filter_obj.extract_parameters()
        # Fallback for filters that don't implement the new method yet
        return [], {}

    def copy(
        self,
        statement: Optional[Union[str, exp.Expression]] = None,
        parameters: Optional[Any] = None,
        dialect: DialectType = None,
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
                _dialect=new_dialect,
                _config=new_config,
                _builder_result_type=self._builder_result_type,
                _existing_state=None,  # Don't use existing state
                **kwargs,
            )

        return SQL(
            new_statement,
            _dialect=new_dialect,
            _config=new_config,
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
            new_statement = self._statement.where(condition_expr)  # pyright: ignore
        else:
            # Wrap in SELECT if needed
            new_statement = exp.Select().from_(self._statement).where(condition_expr)  # pyright: ignore

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
                temp_sql = SQL(final_expr, config=self._config, dialect=self._dialect)
                temp_sql._positional_params = list(self._positional_params)
                temp_sql._named_params = dict(self._named_params)
                result = filter_obj.append_to_statement(temp_sql)
                final_expr = result._statement if isinstance(result, SQL) else result

        # Determine final parameters format
        final_params: Any
        if self._named_params and not self._positional_params:
            # Only named params
            final_params = dict(self._named_params)
        elif self._positional_params and not self._named_params:
            # Always return a list for positional params to maintain sequence type
            final_params = list(self._positional_params)
        elif self._positional_params and self._named_params:
            # Mixed - merge into dict
            final_params = dict(self._named_params)
            # Add positional params with generated names
            for i, param in enumerate(self._positional_params):
                param_name = f"arg_{i}"
                while param_name in final_params:
                    param_name = f"arg_{i}_{id(param)}"
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
        if not self._raw_sql or (self._raw_sql and not self._raw_sql.strip()):
            return ""

        # For scripts, always return the raw SQL to preserve multi-statement scripts
        if self._is_script and self._raw_sql:
            return self._raw_sql
        # If parsing is disabled, return the raw SQL
        if not self._config.enable_parsing and self._raw_sql:
            return self._raw_sql

        # Ensure processed
        self._ensure_processed()
        assert self._processed_state is not None
        return self._processed_state.processed_sql

    @property
    def expression(self) -> Optional[exp.Expression]:
        """Get the final expression."""
        # Return None if parsing is disabled
        if not self._config.enable_parsing:
            return None
        self._ensure_processed()
        assert self._processed_state is not None
        return self._processed_state.processed_expression

    @property
    def parameters(self) -> Any:
        """Get merged parameters."""
        self._ensure_processed()
        assert self._processed_state is not None
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
        # For scripts, return raw SQL directly without processing
        if self._is_script:
            return self.sql, None

        # If parsing is disabled, return raw SQL without transformation
        if not self._config.enable_parsing and self._raw_sql:
            return self._raw_sql, self._raw_parameters

        # Ensure processed
        self._ensure_processed()

        # Get processed SQL and parameters
        assert self._processed_state is not None
        sql = self._processed_state.processed_sql
        params = self._processed_state.merged_parameters

        # Check if parameters were reordered during processing
        if params is not None and hasattr(self, "_processing_context") and self._processing_context:
            parameter_mapping = self._processing_context.metadata.get("parameter_position_mapping")
            if parameter_mapping:
                # Apply parameter reordering based on the mapping
                params = self._reorder_parameters(params, parameter_mapping)

        # If no placeholder style requested, return as-is
        if placeholder_style is None:
            return sql, params

        # Convert to requested placeholder style
        if placeholder_style:
            sql, params = self._convert_placeholder_style(sql, params, placeholder_style)

        # Debug log the final SQL
        logger.debug("Final compiled SQL: '%s'", sql)
        return sql, params

    @staticmethod
    def _reorder_parameters(params: Any, mapping: dict[int, int]) -> Any:
        """Reorder parameters based on the position mapping.

        Args:
            params: Original parameters (list, tuple, or dict)
            mapping: Dict mapping new positions to original positions

        Returns:
            Reordered parameters in the same format as input
        """
        if isinstance(params, (list, tuple)):
            # Create a new list with reordered parameters
            reordered_list = [None] * len(params)  # pyright: ignore
            for new_pos, old_pos in mapping.items():
                if old_pos < len(params):
                    reordered_list[new_pos] = params[old_pos]  # pyright: ignore

            # Handle any unmapped positions
            for i, val in enumerate(reordered_list):
                if val is None and i < len(params) and i not in mapping:
                    # If position wasn't mapped, try to use original
                    reordered_list[i] = params[i]  # pyright: ignore

            # Return in same format as input
            return tuple(reordered_list) if isinstance(params, tuple) else reordered_list

        if isinstance(params, dict):
            # For dict parameters, we need to handle differently
            # If keys are like param_0, param_1, we can reorder them
            if all(key.startswith("param_") and key[6:].isdigit() for key in params):
                reordered_dict: dict[str, Any] = {}
                for new_pos, old_pos in mapping.items():
                    old_key = f"param_{old_pos}"
                    new_key = f"param_{new_pos}"
                    if old_key in params:
                        reordered_dict[new_key] = params[old_key]

                # Add any unmapped parameters
                for key, value in params.items():
                    if key not in reordered_dict and key.startswith("param_"):
                        idx = int(key[6:])
                        if idx not in mapping:
                            reordered_dict[key] = value

                return reordered_dict
            # Can't reorder named parameters, return as-is
            return params
        # Single value or unknown format, return as-is
        return params

    def _convert_placeholder_style(self, sql: str, params: Any, placeholder_style: str) -> tuple[str, Any]:
        """Convert SQL and parameters to the requested placeholder style.

        Args:
            sql: The SQL string to convert
            params: The parameters to convert
            placeholder_style: Target placeholder style

        Returns:
            Tuple of (converted_sql, converted_params)
        """
        # Extract parameter info from current SQL
        converter = self._config.parameter_converter
        param_info = converter.validator.extract_parameters(sql)

        if not param_info:
            return sql, params

        # Use the internal denormalize method to convert to target style
        from sqlspec.statement.parameters import ParameterStyle

        target_style = ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style

        # Replace placeholders in SQL
        sql = self._replace_placeholders_in_sql(sql, param_info, target_style)

        # Convert parameters to appropriate format
        params = self._convert_parameters_format(params, param_info, target_style)

        return sql, params

    def _replace_placeholders_in_sql(self, sql: str, param_info: list[Any], target_style: "ParameterStyle") -> str:
        """Replace placeholders in SQL string with target style placeholders.

        Args:
            sql: The SQL string
            param_info: List of parameter information
            target_style: Target parameter style

        Returns:
            SQL string with replaced placeholders
        """
        # Sort by position in reverse to avoid position shifts
        sorted_params = sorted(param_info, key=lambda p: p.position, reverse=True)

        for p in sorted_params:
            new_placeholder = self._generate_placeholder(p, target_style)
            # Replace the placeholder in SQL
            start = p.position
            end = start + len(p.placeholder_text)
            sql = sql[:start] + new_placeholder + sql[end:]

        return sql

    @staticmethod
    def _generate_placeholder(param: Any, target_style: "ParameterStyle") -> str:
        """Generate a placeholder string for the given parameter style.

        Args:
            param: Parameter information object
            target_style: Target parameter style

        Returns:
            Placeholder string
        """
        if target_style == ParameterStyle.QMARK:
            return "?"
        if target_style == ParameterStyle.NUMERIC:
            # Use 1-based numbering for numeric style
            return f"${param.ordinal + 1}"
        if target_style == ParameterStyle.NAMED_COLON:
            # Use original name if available, otherwise generate one
            # Oracle doesn't like underscores at the start of parameter names
            if param.name and not param.name.isdigit():
                # Use the name if it's not just a number
                return f":{param.name}"
            # Generate a new name for numeric placeholders or missing names
            return f":arg_{param.ordinal}"
        if target_style == ParameterStyle.NAMED_AT:
            # Use @ prefix for BigQuery style
            # BigQuery requires parameter names to start with a letter, not underscore
            return f"@{param.name or f'param_{param.ordinal}'}"
        if target_style == ParameterStyle.POSITIONAL_COLON:
            # Use :1, :2, etc. for Oracle positional style
            return f":{param.ordinal + 1}"
        if target_style == ParameterStyle.POSITIONAL_PYFORMAT:
            # Use %s for positional pyformat
            return "%s"
        if target_style == ParameterStyle.NAMED_PYFORMAT:
            # Use %(name)s for named pyformat
            return f"%({param.name or f'_arg_{param.ordinal}'})s"
        # Keep original for unknown styles
        return str(param.placeholder_text)

    def _convert_parameters_format(self, params: Any, param_info: list[Any], target_style: "ParameterStyle") -> Any:
        """Convert parameters to the appropriate format for the target style.

        Args:
            params: Original parameters
            param_info: List of parameter information
            target_style: Target parameter style

        Returns:
            Converted parameters
        """
        if target_style == ParameterStyle.POSITIONAL_COLON:
            return self._convert_to_positional_colon_format(params, param_info)
        if target_style in {ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_PYFORMAT}:
            return self._convert_to_positional_format(params, param_info)
        if target_style == ParameterStyle.NAMED_COLON:
            return self._convert_to_named_colon_format(params, param_info)
        if target_style == ParameterStyle.NAMED_PYFORMAT:
            return self._convert_to_named_pyformat_format(params, param_info)
        return params

    def _convert_to_positional_colon_format(self, params: Any, param_info: list[Any]) -> Any:
        """Convert to dict format for Oracle positional colon style.

        Oracle's positional colon style uses :1, :2, etc. placeholders and expects
        parameters as a dict with string keys "1", "2", etc.

        For execute_many operations, returns a list of parameter sets.

        Args:
            params: Original parameters
            param_info: List of parameter information

        Returns:
            Dict of parameters with string keys "1", "2", etc., or list for execute_many
        """
        # Special handling for execute_many
        if self._is_many and isinstance(params, list) and params and isinstance(params[0], (list, tuple)):
            # This is execute_many - keep as list but process each item
            return params

        result_dict: dict[str, Any] = {}

        if isinstance(params, (list, tuple)):
            # Convert list/tuple to dict with string keys based on param_info
            if param_info:
                # Check if all param names are numeric (positional colon style)
                all_numeric = all(p.name and p.name.isdigit() for p in param_info)
                if all_numeric:
                    # Sort param_info by numeric name to match list order
                    sorted_params = sorted(param_info, key=lambda p: int(p.name))
                    for i, value in enumerate(params):
                        if i < len(sorted_params):
                            # Map based on numeric order, not SQL appearance order
                            param_name = sorted_params[i].name
                            result_dict[param_name] = value
                        else:
                            # Extra parameters
                            result_dict[str(i + 1)] = value
                else:
                    # Non-numeric names, map by ordinal
                    for i, value in enumerate(params):
                        if i < len(param_info):
                            param_name = param_info[i].name or str(i + 1)
                            result_dict[param_name] = value
                        else:
                            result_dict[str(i + 1)] = value
            else:
                # No param_info, default to 1-based indexing
                for i, value in enumerate(params):
                    result_dict[str(i + 1)] = value
            return result_dict

        if not is_dict(params) and param_info:
            # Single value parameter
            if param_info and param_info[0].name and param_info[0].name.isdigit():
                # Use the actual parameter name from SQL (e.g., "0")
                result_dict[param_info[0].name] = params
            else:
                # Default to "1"
                result_dict["1"] = params
            return result_dict

        if isinstance(params, dict):
            # Check if already in correct format (keys are "1", "2", etc.)
            if all(key.isdigit() for key in params):
                return params

            # Convert from other dict formats
            for p in sorted(param_info, key=lambda x: x.ordinal):
                # Oracle uses 1-based indexing
                oracle_key = str(p.ordinal + 1)
                if p.name and p.name in params:
                    result_dict[oracle_key] = params[p.name]
                elif f"arg_{p.ordinal}" in params:
                    result_dict[oracle_key] = params[f"arg_{p.ordinal}"]
                elif f"param_{p.ordinal}" in params:
                    result_dict[oracle_key] = params[f"param_{p.ordinal}"]
            return result_dict

        return params

    @staticmethod
    def _convert_to_positional_format(params: Any, param_info: list[Any]) -> Any:
        """Convert to list format for positional parameter styles.

        Args:
            params: Original parameters
            param_info: List of parameter information

        Returns:
            List of parameters
        """
        result_list: list[Any] = []
        if is_dict(params):
            for p in param_info:
                if p.name and p.name in params:
                    # Named parameter - get from dict and extract value from TypedParameter if needed
                    val = params[p.name]
                    if hasattr(val, "value"):
                        result_list.append(val.value)
                    else:
                        result_list.append(val)
                elif p.name is None:
                    # Unnamed parameter (qmark style) - look for arg_N
                    arg_key = f"arg_{p.ordinal}"
                    if arg_key in params:
                        # Extract value from TypedParameter if needed
                        val = params[arg_key]
                        if hasattr(val, "value"):
                            result_list.append(val.value)
                        else:
                            result_list.append(val)
                    else:
                        result_list.append(None)
                else:
                    # Named parameter not in dict
                    result_list.append(None)
            return result_list
        if isinstance(params, (list, tuple)):
            for param in params:
                if hasattr(param, "value"):
                    result_list.append(param.value)
                else:
                    result_list.append(param)
            return result_list
        return params

    @staticmethod
    def _convert_to_named_colon_format(params: Any, param_info: list[Any]) -> Any:
        """Convert to dict format for named colon style.

        Args:
            params: Original parameters
            param_info: List of parameter information

        Returns:
            Dict of parameters with generated names
        """
        result_dict: dict[str, Any] = {}
        if is_dict(params):
            # For dict params with matching parameter names, return as-is
            # Otherwise, remap to match the expected names
            if all(p.name in params for p in param_info if p.name):
                return params
            for p in param_info:
                if p.name and p.name in params:
                    result_dict[p.name] = params[p.name]
                elif f"param_{p.ordinal}" in params:
                    # Handle param_N style names
                    # Oracle doesn't like underscores at the start of parameter names
                    result_dict[p.name or f"arg_{p.ordinal}"] = params[f"param_{p.ordinal}"]
            return result_dict
        if isinstance(params, (list, tuple)):
            # Convert list/tuple to dict with parameter names from param_info

            for i, value in enumerate(params):
                if i < len(param_info):
                    p = param_info[i]
                    # Use the actual parameter name if available
                    # Oracle doesn't like underscores at the start of parameter names
                    param_name = p.name or f"arg_{i}"
                    result_dict[param_name] = value
            return result_dict
        return params

    @staticmethod
    def _convert_to_named_pyformat_format(params: Any, param_info: list[Any]) -> Any:
        """Convert to dict format for named pyformat style.

        Args:
            params: Original parameters
            param_info: List of parameter information

        Returns:
            Dict of parameters with names
        """
        if isinstance(params, (list, tuple)):
            # Convert list to dict with generated names
            result_dict: dict[str, Any] = {}
            for i, p in enumerate(param_info):
                if i < len(params):
                    param_name = p.name or f"param_{i}"
                    result_dict[param_name] = params[i]
            return result_dict
        return params

    # Validation properties for compatibility
    @property
    def validation_errors(self) -> list[Any]:
        """Get validation errors."""
        if not self._config.enable_validation:
            return []
        self._ensure_processed()
        assert self._processed_state
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
        validator = self._config.parameter_validator
        if self._config.enable_parsing and self._processed_state:
            sql_for_validation = self.expression.sql(dialect=self._dialect) if self.expression else self.sql  # pyright: ignore
        else:
            sql_for_validation = self.sql
        return validator.extract_parameters(sql_for_validation)

    @property
    def _raw_parameters(self) -> Any:
        """Get raw parameters for compatibility."""
        # Return the original parameters as passed in
        return self._original_parameters

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
                new_statement = result._statement.limit(exp.Placeholder(this=param_name))  # pyright: ignore
            else:
                new_statement = exp.Select().from_(result._statement).limit(exp.Placeholder(this=param_name))  # pyright: ignore
            return result.copy(statement=new_statement)
        if hasattr(self._statement, "limit"):
            new_statement = self._statement.limit(count)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).limit(count)  # pyright: ignore
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
                new_statement = result._statement.offset(exp.Placeholder(this=param_name))  # pyright: ignore
            else:
                new_statement = exp.Select().from_(result._statement).offset(exp.Placeholder(this=param_name))  # pyright: ignore
            return result.copy(statement=new_statement)
        if hasattr(self._statement, "offset"):
            new_statement = self._statement.offset(count)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).offset(count)  # pyright: ignore
        return self.copy(statement=new_statement)

    def order_by(self, expression: exp.Expression) -> "SQL":
        """Add ORDER BY clause."""
        if hasattr(self._statement, "order_by"):
            new_statement = self._statement.order_by(expression)  # pyright: ignore
        else:
            new_statement = exp.Select().from_(self._statement).order_by(expression)  # pyright: ignore
        return self.copy(statement=new_statement)
