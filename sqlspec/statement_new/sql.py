"""New, refactored SQL statement handling."""

from typing import Any, Optional

from sqlspec.statement_new.cache import sql_cache
from sqlspec.statement_new.config import SQLConfig
from sqlspec.statement_new.parameters import ParameterHandler
from sqlspec.statement_new.pipeline import SQLPipeline
from sqlspec.statement_new.protocols import SQLProcessingContext
from sqlspec.statement_new.state import SQLState
from sqlspec.utils.type_guards import supports_where

__all__ = ("SQL", "SQLConfig", "SQLProcessor")


class SQLProcessor:
    """Coordinates the processing of a SQLState object through the pipeline."""

    def __init__(self, pipeline: Optional[SQLPipeline] = None) -> None:
        self.pipeline = pipeline or self._default_pipeline()

    def process(self, state: SQLState) -> SQLState:
        """Process SQL through pipeline."""
        if state.processed:
            return state

        # Use config's dialect if state doesn't have one
        dialect = state.dialect or (state.config.dialect if state.config else None)

        context = SQLProcessingContext(
            initial_sql_string=state.original_sql,
            dialect=dialect,
            config=state.config,
            initial_parameters=state.parameters,
        )
        context = self.pipeline.process(context)

        state.expression = context.current_expression
        state.processing_context = context
        state.validation_errors = [e.message for e in context.validation_errors]
        state.processed = True

        return state

    def _default_pipeline(self) -> SQLPipeline:
        """Get default processing pipeline."""
        # This would be configured with default processors
        return SQLPipeline()


class SQL:
    """Public SQL class - a facade over the internal components."""

    __slots__ = ("_parameter_handler", "_processor", "_state")

    def __init__(
        self,
        sql: str,
        *args: Any,
        dialect: Optional[str] = None,
        config: Optional[SQLConfig] = None,
        _state: Optional[SQLState] = None,
        **kwargs: Any,
    ) -> None:
        if _state:
            self._state = _state
        else:
            parameters: Any
            if args and kwargs:
                parameters = {"positional": args, "named": kwargs}
            elif args:
                parameters = args[0] if len(args) == 1 else list(args)
            elif kwargs:
                parameters = kwargs
            else:
                parameters = None

            # Create config with dialect if provided
            if config is None:
                config = SQLConfig(dialect=dialect)
            elif dialect and not config.dialect:
                # Set dialect in config if not already set
                config = SQLConfig(
                    dialect=dialect,
                    allowed_parameter_styles=config.allowed_parameter_styles,
                    allow_mixed_parameter_styles=config.allow_mixed_parameter_styles,
                    enable_parameter_literal_extraction=config.enable_parameter_literal_extraction,
                    enable_validation=config.enable_validation,
                    enable_transformations=config.enable_transformations,
                    enable_caching=config.enable_caching,
                    cache_max_size=config.cache_max_size,
                )

            self._state = SQLState(
                original_sql=sql, parameters=parameters, dialect=dialect or config.dialect, config=config
            )

        self._processor: Optional[SQLProcessor] = None
        self._parameter_handler = ParameterHandler()

    def _copy(self, **kwargs: Any) -> "SQL":
        """Create a copy of the current SQL object with modifications."""
        state_dict = self._state.__dict__.copy()
        state_dict.update(kwargs)
        new_state = SQLState(**state_dict)
        return SQL("", _state=new_state)

    @property
    def _ensure_processed(self) -> SQLState:
        """Ensure SQL is processed through the pipeline (lazy evaluation)."""
        if self._state.processed:
            return self._state

        initial_cache_key = self._state.cache_key()
        cached_state = sql_cache.get(initial_cache_key)

        if cached_state:
            self._state = cached_state
            return self._state

        if self._processor is None:
            self._processor = SQLProcessor()
        self._state = self._processor.process(self._state)

        # Use initial cache key for storage so future lookups work
        sql_cache.set(initial_cache_key, self._state)
        return self._state

    @property
    def sql(self) -> str:
        """Get processed SQL string."""
        state = self._ensure_processed
        if state.expression:
            return state.expression.sql(dialect=state.dialect)
        return state.original_sql

    @property
    def parameters(self) -> Any:
        """Get processed parameters."""
        return self._ensure_processed.parameters

    def compile(self, style: Optional[str] = None) -> tuple[str, Any]:
        """Compile to specific parameter style with caching."""
        state = self._ensure_processed
        style_key = style or "default"
        # Use a more robust cache key for compiled output
        compiled_cache_key = f"{state.cache_key()}:{style_key}"
        if compiled_cache_key in state._compiled_cache:
            return state._compiled_cache[compiled_cache_key]

        sql_str = self.sql
        params = self.parameters

        if style:
            # Extract current parameter style from the SQL
            param_info = self._parameter_handler.extract_parameters(sql_str)
            if param_info:
                # Determine the source style
                detected_styles = {p.style for p in param_info}
                if len(detected_styles) == 1:
                    from_style = detected_styles.pop()
                    # Convert to target style
                    from sqlspec.statement_new.parameters import ParameterStyle

                    try:
                        target_style = ParameterStyle(style)
                        converted = self._parameter_handler.convert_parameters_direct(
                            sql_str, from_style, target_style, use_sqlglot=True
                        )
                        sql_str = converted.transformed_sql
                    except ValueError:
                        # Invalid style specified, keep original
                        pass

        result = (sql_str, params)
        state._compiled_cache[compiled_cache_key] = result
        return result

    def where(self, condition: str, **params: Any) -> "SQL":
        """Add WHERE clause - returns new instance."""
        state = self._ensure_processed
        if not state.expression:
            # Cannot add WHERE to a non-expression based SQL object
            return self

        new_expression = state.expression.copy()
        if supports_where(new_expression):
            new_expression = new_expression.where(condition)

        new_params = self.parameters.copy() if self.parameters else {}
        if isinstance(new_params, dict):
            new_params.update(params)

        return self._copy(expression=new_expression, parameters=new_params, processed=False)
