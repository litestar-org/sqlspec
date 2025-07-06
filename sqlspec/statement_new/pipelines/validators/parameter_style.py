"""Parameter style validation for SQL statements."""

import logging
from typing import TYPE_CHECKING

from sqlglot import exp

from sqlspec.exceptions import RiskLevel, SQLValidationError
from sqlspec.statement_new.protocols import ProcessorPhase, SQLProcessor, ValidationError
from sqlspec.utils.type_guards import is_dict

if TYPE_CHECKING:
    from sqlspec.statement_new.pipelines import SQLProcessingContext

logger = logging.getLogger("sqlspec.validators.parameter_style")

__all__ = ("ParameterStyleValidator",)


class UnsupportedParameterStyleError(SQLValidationError):
    """Raised when a parameter style is not supported by the current database."""


class MixedParameterStyleError(SQLValidationError):
    """Raised when mixed parameter styles are detected but not allowed."""


class ParameterStyleValidator(SQLProcessor):
    """Validates that parameter styles are supported by the database configuration."""

    phase = ProcessorPhase.VALIDATE

    def __init__(self, risk_level: "RiskLevel" = RiskLevel.HIGH, fail_on_violation: bool = True) -> None:
        self.risk_level = risk_level
        self.fail_on_violation = fail_on_violation

    def process(self, context: "SQLProcessingContext") -> "SQLProcessingContext":
        if context.current_expression is None:
            return context
        self.validate(context.current_expression, context)
        return context

    def validate(self, expression: exp.Expression, context: "SQLProcessingContext") -> None:
        config = context.config
        param_info = context.parameter_info

        if config.allowed_parameter_styles is not None and param_info:
            unique_styles = {p.style for p in param_info}
            if len(unique_styles) > 1 and not config.allow_mixed_parameter_styles:
                self._report_error(context, expression, f"Mixed parameter styles detected: {unique_styles}")

            disallowed_styles = {str(s) for s in unique_styles if not config.validate_parameter_style(s)}
            if disallowed_styles:
                self._report_error(context, expression, f"Unsupported parameter styles: {disallowed_styles}")

        if param_info and (context.merged_parameters is not None or context.input_sql_had_placeholders):
            self._validate_missing_parameters(context, expression)

    def _report_error(self, context: "SQLProcessingContext", expression: exp.Expression, message: str) -> None:
        if self.fail_on_violation:
            raise MixedParameterStyleError(message)
        error = ValidationError(
            message=message,
            code="parameter-style-error",
            risk_level=self.risk_level,
            processor=self.__class__.__name__,
            expression=expression,
        )
        context.validation_errors.append(error)

    def _validate_missing_parameters(self, context: "SQLProcessingContext", expression: exp.Expression) -> None:
        param_info = context.parameter_info
        merged_params = context.merged_parameters

        if merged_params is None:
            missing = [p.name or p.placeholder_text for p in param_info]
            if missing:
                self._report_error(context, expression, f"Missing required parameters: {missing}")
        elif isinstance(merged_params, (list, tuple)):
            if len(merged_params) < len(param_info):
                self._report_error(
                    context, expression, f"Expected {len(param_info)} parameters but got {len(merged_params)}"
                )
        elif is_dict(merged_params):
            missing = [p.name for p in param_info if p.name and p.name not in merged_params]
            if missing:
                self._report_error(context, expression, f"Missing required named parameters: {missing}")
