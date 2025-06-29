"""SQL compilation logic separated from the main SQL class."""

from typing import TYPE_CHECKING, Any, Optional, Union

import sqlglot.expressions as exp

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLCompilationError
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.pipelines import SQLProcessingContext, StatementPipeline
from sqlspec.statement.sql import SQLConfig


class SQLCompiler:
    """Handles SQL compilation and pipeline processing."""

    def __init__(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        parameter_manager: Optional[Any] = None,
        is_many: bool = False,
        is_script: bool = False,
    ) -> None:
        self.expression = expression
        self.dialect = dialect
        self.parameter_manager = parameter_manager
        self.is_many = is_many
        self.is_script = is_script

    def compile(self, placeholder_style: Optional[str] = None) -> tuple[str, Any]:
        """Compile SQL and parameters."""
        if self.is_script:
            return self.expression.sql(dialect=self.dialect), None

        pipeline = self._get_pipeline()
        context = self._get_processing_context(pipeline)
        processed_expr = self._execute_pipeline(pipeline, context)

        sql = processed_expr.sql(dialect=self.dialect, comments=False)
        params = self._get_compiled_parameters(context, placeholder_style)

        return sql, params

    def to_sql(self, placeholder_style: Optional[str] = None) -> str:
        """Get the SQL string with a specific placeholder style."""
        sql, _ = self.compile(placeholder_style=placeholder_style)
        return sql

    def get_parameters(self, style: Union[ParameterStyle, str, None] = None) -> Any:
        """Get the parameters in a specific style."""
        _, params = self.compile(placeholder_style=str(style) if style else None)
        return params

    def _get_pipeline(self) -> StatementPipeline:
        """Get the statement pipeline."""
        # This can be extended to use a configured pipeline from SQLConfig
        return StatementPipeline()

    def _get_processing_context(self, pipeline: StatementPipeline) -> SQLProcessingContext:
        """Get the processing context."""

        # Create a proper context with all required fields
        context = SQLProcessingContext(
            initial_sql_string=self.expression.sql(dialect=self.dialect),
            dialect=self.dialect,
            config=SQLConfig(dialect=self.dialect),
        )

        # Set the expression fields
        context.initial_expression = self.expression
        context.current_expression = self.expression

        # Set the parameter fields
        if self.parameter_manager:
            context.merged_parameters = self.parameter_manager.named_parameters
            context.initial_parameters = self.parameter_manager.positional_parameters
            context.initial_kwargs = self.parameter_manager.named_parameters

        return context

    def _execute_pipeline(self, pipeline: StatementPipeline, context: SQLProcessingContext) -> exp.Expression:
        """Execute the processing pipeline."""
        try:
            result = pipeline.execute_pipeline(context)
        except Exception as e:
            msg = f"Failed to compile SQL: {context.initial_sql_string}"
            raise SQLCompilationError(msg) from e
        else:
            return result.expression

    def _get_compiled_parameters(self, context: SQLProcessingContext, placeholder_style: Optional[str]) -> Any:
        """Get compiled parameters in target style."""
        if not self.parameter_manager:
            return None

        if placeholder_style:
            style = ParameterStyle(placeholder_style) if isinstance(placeholder_style, str) else placeholder_style
        else:
            # Default to named colon style
            style = ParameterStyle.NAMED_COLON
        return self.parameter_manager.get_compiled_parameters(context.parameter_info, style)
