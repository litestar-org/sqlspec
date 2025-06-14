"""Parameter style validation for SQL statements."""

import logging
from typing import TYPE_CHECKING

from sqlglot import exp

from sqlspec.exceptions import RiskLevel, SQLValidationError
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.result_types import ValidationError

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

logger = logging.getLogger("sqlspec.validators.parameter_style")

__all__ = ("ParameterStyleValidator",)


class UnsupportedParameterStyleError(SQLValidationError):
    """Raised when a parameter style is not supported by the current database."""


class MixedParameterStyleError(SQLValidationError):
    """Raised when mixed parameter styles are detected but not allowed."""


class ParameterStyleValidator(ProcessorProtocol):
    """Validates that parameter styles are supported by the database configuration.

    This validator checks:
    1. Whether detected parameter styles are in the allowed list
    2. Whether mixed parameter styles are used when not allowed
    3. Provides helpful error messages about supported styles
    """

    def __init__(self, risk_level: "RiskLevel" = RiskLevel.HIGH, fail_on_violation: bool = True) -> None:
        """Initialize the parameter style validator.

        Args:
            risk_level: Risk level for unsupported parameter styles
            fail_on_violation: Whether to raise exception on violation
        """
        self.risk_level = risk_level
        self.fail_on_violation = fail_on_violation

    def process(self, expression: exp.Expression, context: "SQLProcessingContext") -> None:
        """Validate parameter styles in SQL.

        Args:
            context: SQL processing context with config

        Returns:
            A ProcessorResult with the outcome of the validation.
        """
        if context.current_expression is None:
            error = ValidationError(
                message="ParameterStyleValidator received no expression.",
                code="no-expression",
                risk_level=RiskLevel.CRITICAL,
                processor="ParameterStyleValidator",
                expression=None,
            )
            context.validation_errors.append(error)
            return

        try:
            config = context.config

            if config.allowed_parameter_styles is None:
                return

            param_info = context.parameter_info
            if not param_info:
                return

            unique_styles = {p.style for p in param_info}
            if len(unique_styles) > 1 and not config.allow_mixed_parameter_styles:
                detected_styles = ", ".join(sorted(str(s) for s in unique_styles))
                msg = f"Mixed parameter styles detected ({detected_styles}) but not allowed."
                if self.fail_on_violation:
                    self._raise_mixed_style_error(msg)
                error = ValidationError(
                    message=msg,
                    code="mixed-parameter-styles",
                    risk_level=self.risk_level,
                    processor="ParameterStyleValidator",
                    expression=expression,
                )
                context.validation_errors.append(error)

            disallowed_styles = {str(s) for s in unique_styles if not config.validate_parameter_style(s)}
            if disallowed_styles:
                disallowed_str = ", ".join(sorted(disallowed_styles))
                allowed_str = ", ".join(config.allowed_parameter_styles)
                msg = f"Parameter style(s) {disallowed_str} not supported. Allowed: {allowed_str}"
                if self.fail_on_violation:
                    self._raise_unsupported_style_error(msg)
                error = ValidationError(
                    message=msg,
                    code="unsupported-parameter-style",
                    risk_level=self.risk_level,
                    processor="ParameterStyleValidator",
                    expression=expression,
                )
                context.validation_errors.append(error)

        except (UnsupportedParameterStyleError, MixedParameterStyleError):
            raise
        except Exception as e:
            logger.warning("Parameter style validation failed: %s", e)
            error = ValidationError(
                message=f"Parameter style validation failed: {e}",
                code="validation-error",
                risk_level=RiskLevel.LOW,
                processor="ParameterStyleValidator",
                expression=expression,
            )
            context.validation_errors.append(error)

    @staticmethod
    def _raise_mixed_style_error(msg: "str") -> "None":
        """Raise MixedParameterStyleError with the given message."""
        raise MixedParameterStyleError(msg)

    @staticmethod
    def _raise_unsupported_style_error(msg: "str") -> "None":
        """Raise UnsupportedParameterStyleError with the given message."""
        raise UnsupportedParameterStyleError(msg)
