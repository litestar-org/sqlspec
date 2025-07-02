"""SQL compilation logic separated from the main SQL class."""

from typing import TYPE_CHECKING, Any, Optional, Union, cast

import sqlglot.expressions as exp

from sqlspec.exceptions import SQLCompilationError
from sqlspec.statement.parameters import ParameterConverter, ParameterStyle
from sqlspec.statement.pipelines import SQLProcessingContext, StatementPipeline
from sqlspec.statement.sql import SQLConfig
from sqlspec.utils.cached_property import CachedProperty

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.protocols import ProcessorProtocol
    from sqlspec.statement.parameter_manager import ParameterManager


__all__ = ("SQLCompiler",)


class SQLCompiler:
    """Handles SQL compilation and pipeline processing."""

    def __init__(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        parameter_manager: "Optional[ParameterManager]" = None,
        is_script: bool = False,
        original_sql: Optional[str] = None,
        config: Optional[SQLConfig] = None,
    ) -> None:
        self.expression = expression
        self.dialect = dialect
        self.parameter_manager = parameter_manager
        self.is_script = is_script
        self._original_sql = original_sql
        self.config = config or SQLConfig(dialect=dialect)

    @CachedProperty
    def _pipeline(self) -> StatementPipeline:
        """Get the statement pipeline."""
        validators: list[ProcessorProtocol] = []

        if self.config.enable_validation and self.config.allowed_parameter_styles is not None:
            from sqlspec.statement.pipelines.validators._parameter_style import ParameterStyleValidator

            # In strict mode, fail on violations
            validators.append(ParameterStyleValidator(fail_on_violation=self.config.strict_mode))

        return StatementPipeline(validators=validators)

    @CachedProperty
    def _context(self) -> SQLProcessingContext:
        """Get the processing context."""
        if isinstance(self.expression, exp.Anonymous) and self.expression.this:
            sql_string = str(self.expression.this)
        else:
            sql_string = self.expression.sql(dialect=self.dialect)

        context = SQLProcessingContext(initial_sql_string=sql_string, dialect=self.dialect, config=self.config)
        context.initial_expression = self.expression
        context.current_expression = self.expression

        from sqlspec.statement.parameters import ParameterValidator

        validator = ParameterValidator()
        context.parameter_info = validator.extract_parameters(sql_string)

        if self.parameter_manager:
            if self.parameter_manager.positional_parameters:
                context.merged_parameters = self.parameter_manager.positional_parameters
                context.initial_parameters = self.parameter_manager.positional_parameters
            elif self.parameter_manager.named_parameters:
                context.merged_parameters = self.parameter_manager.named_parameters
                context.initial_kwargs = self.parameter_manager.named_parameters
            context.initial_parameters = self.parameter_manager.positional_parameters
            context.initial_kwargs = self.parameter_manager.named_parameters
        return context

    @CachedProperty
    def _processed_expr(self) -> exp.Expression:
        """Execute the processing pipeline and cache the result."""
        try:
            result = self._pipeline.execute_pipeline(self._context)
        except Exception as e:
            msg = f"Failed to compile SQL: {self._context.initial_sql_string}"
            raise SQLCompilationError(msg) from e
        else:
            return cast("exp.Expression", result.expression)

    @CachedProperty
    def _compiled_sql(self) -> str:
        """Get the compiled SQL string."""
        if self.is_script:
            return str(self._original_sql or self.expression.sql(dialect=self.dialect))
        # Always go through the pipeline to ensure validation runs
        processed = self._processed_expr
        if isinstance(processed, exp.Anonymous) and processed.this:
            return str(processed.this)
        return str(processed.sql(dialect=self.dialect, comments=False))

    def compile(self, placeholder_style: Optional[str] = None) -> tuple[str, Any]:
        """Compile SQL and parameters."""
        if self.is_script:
            return self._compiled_sql, None

        sql = self.to_sql(placeholder_style)
        params = self._get_compiled_parameters(placeholder_style)
        return sql, params

    def to_sql(self, placeholder_style: Optional[str] = None) -> str:
        """Get the SQL string with a specific placeholder style."""
        if placeholder_style is None or self.is_script:
            return cast("str", self._compiled_sql)

        converter = ParameterConverter()
        sql = self._compiled_sql

        target_style = ParameterStyle(placeholder_style)
        return converter.convert_placeholders(sql, target_style, self._context.parameter_info)

    def get_parameters(self, style: Union[ParameterStyle, str, None] = None) -> Any:
        """Get the parameters in a specific style."""
        if self.is_script:
            return None
        return cast("Any", self._get_compiled_parameters(str(style) if style else None))

    def _get_compiled_parameters(self, placeholder_style: Optional[str]) -> Any:
        """Get compiled parameters in target style."""
        if not self.parameter_manager:
            return None

        # This ensures the pipeline has run and context is populated
        _ = self._processed_expr

        style_enum = ParameterStyle(placeholder_style) if placeholder_style else ParameterStyle.NAMED_COLON
        return self.parameter_manager.get_compiled_parameters(self._context.parameter_info, style_enum)
