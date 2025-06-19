"""Parameter style validation for SQL statements."""

import logging
from typing import TYPE_CHECKING

from sqlglot import exp

from sqlspec.exceptions import MissingParameterError, RiskLevel, SQLValidationError
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.result_types import ValidationError
from sqlspec.typing import is_dict

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
            expression: The SQL expression being validated
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
            param_info = context.parameter_info

            # First check parameter styles if configured
            has_style_errors = False
            if config.allowed_parameter_styles is not None and param_info:
                unique_styles = {p.style for p in param_info}

                # Check for mixed styles first (before checking individual styles)
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
                    has_style_errors = True

                # Check for disallowed styles
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
                    has_style_errors = True

            # Check for missing parameters if:
            # 1. We have parameter info
            # 2. Style validation is enabled (allowed_parameter_styles is not None)
            # 3. No style errors were found
            if param_info and config.allowed_parameter_styles is not None and not has_style_errors:
                # Check for missing parameters
                self._validate_missing_parameters(context, expression)

        except (UnsupportedParameterStyleError, MixedParameterStyleError, MissingParameterError):
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

    def _validate_missing_parameters(self, context: "SQLProcessingContext", expression: exp.Expression) -> None:
        """Validate that all required parameters have values provided.

        Args:
            context: SQL processing context
            expression: The SQL expression being validated
        """
        # Get parameter info from SQL
        param_info = context.parameter_info
        if not param_info:
            return

        # Get merged parameters
        merged_params = context.merged_parameters

        # Special handling for Oracle numeric parameters with single values
        # When we have Oracle numeric params and a single positional value,
        # it gets unwrapped from [value] to value, but we need to treat it as a list
        has_positional_colon = any(p.style.value == "positional_colon" for p in param_info)
        if has_positional_colon and not isinstance(merged_params, (list, tuple, dict)) and merged_params is not None:
            # Single value with Oracle numeric params - treat as single-element list
            merged_params = [merged_params]

        # Handle different parameter formats
        if merged_params is None:
            # No parameters provided at all
            if param_info:
                missing = [p.name for p in param_info]
                msg = f"Missing required parameters: {', '.join(missing)}"
                if self.fail_on_violation:
                    raise MissingParameterError(msg)
                error = ValidationError(
                    message=msg,
                    code="missing-parameters",
                    risk_level=self.risk_level,
                    processor="ParameterStyleValidator",
                    expression=expression,
                )
                context.validation_errors.append(error)
        elif isinstance(merged_params, (list, tuple)):
            # Positional parameters - check count
            required_count = len(param_info)
            provided_count = len(merged_params)

            # Check for mixed parameter styles (e.g., Oracle numeric + named)
            has_named = any(p.style.value in {"named_colon", "named_at"} for p in param_info)
            if has_named:
                # We have named parameters but only positional values provided
                missing_named = [p.name for p in param_info if p.style.value in {"named_colon", "named_at"}]
                if missing_named:
                    msg = f"Missing required parameters: {', '.join(missing_named)}"
                    if self.fail_on_violation:
                        raise MissingParameterError(msg)
                    error = ValidationError(
                        message=msg,
                        code="missing-parameters",
                        risk_level=self.risk_level,
                        processor="ParameterStyleValidator",
                        expression=expression,
                    )
                    context.validation_errors.append(error)
                    return  # Don't continue checking positional params

            # Check for Oracle numeric parameters
            if has_positional_colon:
                # For Oracle numeric, we need to check if all required indices have values
                missing_indices = []
                for p in param_info:
                    # Skip non-Oracle numeric parameters
                    if p.style.value != "positional_colon" or p.name is None:
                        continue
                    try:
                        # Oracle numeric parameter names are the indices
                        idx = int(p.name)
                        # Check if this index has a value in the list
                        # For 0-based: :0, :1 maps to list indices 0, 1
                        # For 1-based: :1, :2 maps to list indices 0, 1 (subtract 1)
                        # We'll check both interpretations
                        if idx < provided_count:
                            # 0-based interpretation - direct index
                            continue
                        if idx > 0 and (idx - 1) < provided_count:
                            # 1-based interpretation - subtract 1
                            continue
                        # Missing this parameter
                        missing_indices.append(p.name)
                    except (ValueError, TypeError):
                        # Not a numeric parameter name
                        pass

                if missing_indices:
                    msg = f"Missing required parameters: :{', :'.join(missing_indices)}"
                    if self.fail_on_violation:
                        raise MissingParameterError(msg)
                    error = ValidationError(
                        message=msg,
                        code="missing-parameters",
                        risk_level=self.risk_level,
                        processor="ParameterStyleValidator",
                        expression=expression,
                    )
                    context.validation_errors.append(error)
            elif provided_count < required_count:
                # Regular positional parameters - simple count check
                msg = f"Expected {required_count} parameters but got {provided_count}"
                if self.fail_on_violation:
                    raise MissingParameterError(msg)
                error = ValidationError(
                    message=msg,
                    code="missing-parameters",
                    risk_level=self.risk_level,
                    processor="ParameterStyleValidator",
                    expression=expression,
                )
                context.validation_errors.append(error)
        elif is_dict(merged_params):
            # Named parameters - check keys
            missing = []
            for p in param_info:
                param_name = p.name
                # Check if this parameter exists in merged_params
                if param_name not in merged_params:
                    # Also check for _arg_ style names for positional params
                    if not any(key.startswith(("_arg_", "param_")) for key in merged_params):
                        missing.append(param_name)
                    elif p.style.value not in {"qmark", "numeric"}:
                        # For named parameters, we need the exact name
                        missing.append(param_name)

            if missing:
                msg = f"Missing required parameters: {', '.join(missing)}"
                if self.fail_on_violation:
                    raise MissingParameterError(msg)
                error = ValidationError(
                    message=msg,
                    code="missing-parameters",
                    risk_level=self.risk_level,
                    processor="ParameterStyleValidator",
                    expression=expression,
                )
                context.validation_errors.append(error)
        # Single value - only valid if we have exactly one parameter
        elif len(param_info) > 1:
            missing = [p.name for p in param_info[1:]]
            msg = f"Missing required parameters: {', '.join(missing)}"
            if self.fail_on_violation:
                raise MissingParameterError(msg)
            error = ValidationError(
                message=msg,
                code="missing-parameters",
                risk_level=self.risk_level,
                processor="ParameterStyleValidator",
                expression=expression,
            )
            context.validation_errors.append(error)
