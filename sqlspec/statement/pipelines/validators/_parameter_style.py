"""Parameter style validation for SQL statements."""

import logging
from typing import TYPE_CHECKING

from sqlglot import exp

from sqlspec.exceptions import RiskLevel, SQLValidationError
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.results import ProcessorResult, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

logger = logging.getLogger("sqlspec.validators.parameter_style")

__all__ = ("ParameterStyleValidator",)


class UnsupportedParameterStyleError(SQLValidationError):
    """Raised when a parameter style is not supported by the current database."""


class MixedParameterStyleError(SQLValidationError):
    """Raised when mixed parameter styles are detected but not allowed."""


class ParameterStyleValidator(ProcessorProtocol[exp.Expression]):
    """Validates that parameter styles are supported by the database configuration.

    This validator checks:
    1. Whether detected parameter styles are in the allowed list
    2. Whether mixed parameter styles are used when not allowed
    3. Provides helpful error messages about supported styles
    """

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.HIGH,
        fail_on_violation: bool = True,
    ) -> None:
        """Initialize the parameter style validator.

        Args:
            risk_level: Risk level for unsupported parameter styles
            fail_on_violation: Whether to raise exception on violation
        """
        self.risk_level = risk_level
        self.fail_on_violation = fail_on_violation

    def process(self, context: "SQLProcessingContext") -> "ProcessorResult":
        """Validate parameter styles in SQL.

        Args:
            context: SQL processing context with config

        Returns:
            A ProcessorResult with the outcome of the validation.
        """
        if context.current_expression is None:
            return ProcessorResult(
                expression=exp.Placeholder(),
                validation_result=ValidationResult(
                    is_safe=False,
                    risk_level=RiskLevel.CRITICAL,
                    issues=["ParameterStyleValidator received no expression."],
                ),
            )

        try:
            config = context.config
            issues = []

            if config.allowed_parameter_styles is None:
                return ProcessorResult(
                    expression=context.current_expression,
                    validation_result=ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP),
                )

            param_info = context.parameter_info
            if not param_info:
                return ProcessorResult(
                    expression=context.current_expression,
                    validation_result=ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP),
                )

            unique_styles = {p.style for p in param_info}
            if len(unique_styles) > 1 and not config.allow_mixed_parameter_styles:
                detected_styles = ", ".join(sorted(str(s) for s in unique_styles))
                msg = f"Mixed parameter styles detected ({detected_styles}) but not allowed."
                if self.fail_on_violation:
                    self._raise_mixed_style_error(msg)
                issues.append(msg)

            disallowed_styles = {str(s) for s in unique_styles if not config.validate_parameter_style(s)}
            if disallowed_styles:
                disallowed_str = ", ".join(sorted(disallowed_styles))
                allowed_str = ", ".join(config.allowed_parameter_styles)
                msg = f"Parameter style(s) {disallowed_str} not supported. Allowed: {allowed_str}"
                if self.fail_on_violation:
                    self._raise_unsupported_style_error(msg)
                issues.append(msg)

            validation_result = (
                ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues)
                if issues
                else ValidationResult(is_safe=True, risk_level=RiskLevel.SKIP)
            )

        except (UnsupportedParameterStyleError, MixedParameterStyleError):
            raise
        except Exception as e:
            logger.warning("Parameter style validation failed: %s", e)
            validation_result = ValidationResult(
                is_safe=True, risk_level=RiskLevel.SKIP, warnings=[f"Parameter style validation failed: {e}"]
            )

        return ProcessorResult(expression=context.current_expression, validation_result=validation_result)

    def _raise_mixed_style_error(self, msg: "str") -> "None":
        """Raise MixedParameterStyleError with the given message."""
        raise MixedParameterStyleError(msg)

    def _raise_unsupported_style_error(self, msg: "str") -> "None":
        """Raise UnsupportedParameterStyleError with the given message."""
        raise UnsupportedParameterStyleError(msg)
