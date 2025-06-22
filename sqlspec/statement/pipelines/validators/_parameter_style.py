"""Parameter style validation for SQL statements."""

import logging
from typing import TYPE_CHECKING, Any, Optional, Union

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

    def process(self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext") -> None:
        """Validate parameter styles in SQL.

        Args:
            expression: The SQL expression being validated
            context: SQL processing context with config

        Returns:
            A ProcessorResult with the outcome of the validation.
        """
        if expression is None:
            return

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
                    detected_style_strs = [str(s) for s in unique_styles]
                    detected_styles = ", ".join(sorted(detected_style_strs))
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
                    # Defensive handling to avoid "expected str instance, NoneType found"
                    if config.allowed_parameter_styles:
                        allowed_styles_strs = [str(s) for s in config.allowed_parameter_styles]
                        allowed_str = ", ".join(allowed_styles_strs)
                        msg = f"Parameter style(s) {disallowed_str} not supported. Allowed: {allowed_str}"
                    else:
                        msg = f"Parameter style(s) {disallowed_str} not supported."

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
            # 4. We have merged parameters OR the original SQL had placeholders
            logger.debug(
                "Checking missing parameters: param_info=%s, extracted=%s, had_placeholders=%s, merged=%s",
                len(param_info) if param_info else 0,
                len(context.extracted_parameters_from_pipeline) if context.extracted_parameters_from_pipeline else 0,
                context.input_sql_had_placeholders,
                context.merged_parameters is not None,
            )
            # Skip validation if we have no merged parameters and the SQL didn't originally have placeholders
            # This handles the case where literals were parameterized by transformers
            if (
                param_info
                and config.allowed_parameter_styles is not None
                and not has_style_errors
                and (context.merged_parameters is not None or context.input_sql_had_placeholders)
            ):
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
        """Validate that all required parameters have values provided."""
        param_info = context.parameter_info
        if not param_info:
            return

        merged_params = self._prepare_merged_parameters(context, param_info)

        if merged_params is None:
            self._handle_no_parameters(context, expression, param_info)
        elif isinstance(merged_params, (list, tuple)):
            self._handle_positional_parameters(context, expression, param_info, merged_params)
        elif is_dict(merged_params):
            self._handle_named_parameters(context, expression, param_info, merged_params)
        elif len(param_info) > 1:
            self._handle_single_value_multiple_params(context, expression, param_info)

    @staticmethod
    def _prepare_merged_parameters(context: "SQLProcessingContext", param_info: list[Any]) -> Any:
        """Prepare merged parameters for validation."""
        merged_params = context.merged_parameters

        # If we have extracted parameters from transformers (like ParameterizeLiterals),
        # use those for validation instead of the original merged_parameters
        if context.extracted_parameters_from_pipeline and not context.input_sql_had_placeholders:
            # Use extracted parameters as they represent the actual values to be used
            merged_params = context.extracted_parameters_from_pipeline
        has_positional_colon = any(p.style.value == "positional_colon" for p in param_info)
        if has_positional_colon and not isinstance(merged_params, (list, tuple, dict)) and merged_params is not None:
            return [merged_params]
        return merged_params

    def _report_error(self, context: "SQLProcessingContext", expression: exp.Expression, message: str) -> None:
        """Report a missing parameter error."""
        if self.fail_on_violation:
            raise MissingParameterError(message)
        error = ValidationError(
            message=message,
            code="missing-parameters",
            risk_level=self.risk_level,
            processor="ParameterStyleValidator",
            expression=expression,
        )
        context.validation_errors.append(error)

    def _handle_no_parameters(
        self, context: "SQLProcessingContext", expression: exp.Expression, param_info: list[Any]
    ) -> None:
        """Handle validation when no parameters are provided."""
        if context.extracted_parameters_from_pipeline:
            return
        missing = [p.name or p.placeholder_text or f"param_{p.ordinal}" for p in param_info]
        msg = f"Missing required parameters: {', '.join(str(m) for m in missing)}"
        self._report_error(context, expression, msg)

    def _handle_positional_parameters(
        self,
        context: "SQLProcessingContext",
        expression: exp.Expression,
        param_info: list[Any],
        merged_params: "Union[list[Any], tuple[Any, ...]]",
    ) -> None:
        """Handle validation for positional parameters."""
        has_named = any(p.style.value in {"named_colon", "named_at"} for p in param_info)
        if has_named:
            missing_named = [
                p.name or p.placeholder_text for p in param_info if p.style.value in {"named_colon", "named_at"}
            ]
            if missing_named:
                msg = f"Missing required parameters: {', '.join(str(m) for m in missing_named if m)}"
                self._report_error(context, expression, msg)
                return

        has_positional_colon = any(p.style.value == "positional_colon" for p in param_info)
        if has_positional_colon:
            self._validate_oracle_numeric_params(context, expression, param_info, merged_params)
        elif len(merged_params) < len(param_info):
            msg = f"Expected {len(param_info)} parameters but got {len(merged_params)}"
            self._report_error(context, expression, msg)

    def _validate_oracle_numeric_params(
        self,
        context: "SQLProcessingContext",
        expression: exp.Expression,
        param_info: list[Any],
        merged_params: "Union[list[Any], tuple[Any, ...]]",
    ) -> None:
        """Validate Oracle-style numeric parameters."""
        missing_indices: list[str] = []
        provided_count = len(merged_params)
        for p in param_info:
            if p.style.value != "positional_colon" or not p.name:
                continue
            try:
                idx = int(p.name)
                if not (idx < provided_count or (idx > 0 and (idx - 1) < provided_count)):
                    missing_indices.append(p.name)
            except (ValueError, TypeError):
                pass
        if missing_indices:
            msg = f"Missing required parameters: :{', :'.join(missing_indices)}"
            self._report_error(context, expression, msg)

    def _handle_named_parameters(
        self,
        context: "SQLProcessingContext",
        expression: exp.Expression,
        param_info: list[Any],
        merged_params: dict[str, Any],
    ) -> None:
        """Handle validation for named parameters."""
        missing: list[str] = []
        for p in param_info:
            param_name = p.name
            if param_name not in merged_params:
                is_synthetic = any(key.startswith(("_arg_", "param_")) for key in merged_params)
                is_named_style = p.style.value not in {"qmark", "numeric"}
                if (not is_synthetic or is_named_style) and param_name:
                    missing.append(param_name)

        if missing:
            msg = f"Missing required parameters: {', '.join(missing)}"
            self._report_error(context, expression, msg)

    def _handle_single_value_multiple_params(
        self, context: "SQLProcessingContext", expression: exp.Expression, param_info: list[Any]
    ) -> None:
        """Handle validation for a single value provided for multiple parameters."""
        missing = [p.name or p.placeholder_text or f"param_{p.ordinal}" for p in param_info[1:]]
        msg = f"Missing required parameters: {', '.join(str(m) for m in missing)}"
        self._report_error(context, expression, msg)
