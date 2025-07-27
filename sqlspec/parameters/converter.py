"""Parameter conversion logic for SQL placeholders.

This module handles the conversion between different parameter styles.
"""

from typing import Any, Optional

from sqlspec.parameters.types import (
    ConvertedParameters,
    ParameterInfo,
    ParameterStyle,
    ParameterStyleConversionState,
    TypedParameter,
)
from sqlspec.parameters.validator import ParameterValidator

__all__ = ("ParameterConverter",)


class ParameterConverter:
    """Parameter parameter conversion with caching and validation."""

    __slots__ = ("validator",)

    def __init__(self) -> None:
        """Initialize converter with validator."""
        self.validator = ParameterValidator()

    def convert_placeholders(
        self, sql: str, target_style: ParameterStyle, parameter_info: Optional[list[ParameterInfo]] = None
    ) -> str:
        """Convert SQL placeholders to a target style.

        Args:
            sql: The SQL string with placeholders
            target_style: The target parameter style to convert to
            parameter_info: Optional list of parameter info (will be extracted if not provided)

        Returns:
            SQL string with converted placeholders
        """
        if parameter_info is None:
            parameter_info = self.validator.extract_parameters(sql)

        if not parameter_info:
            return sql

        result_parts = []
        current_pos = 0

        for i, param in enumerate(parameter_info):
            result_parts.append(sql[current_pos : param.position])

            if target_style == ParameterStyle.QMARK:
                placeholder = "?"
            elif target_style == ParameterStyle.NUMERIC:
                placeholder = f"${i + 1}"
            elif target_style == ParameterStyle.POSITIONAL_PYFORMAT:
                placeholder = "%s"
            elif target_style == ParameterStyle.NAMED_COLON:
                if param.style in {
                    ParameterStyle.POSITIONAL_COLON,
                    ParameterStyle.QMARK,
                    ParameterStyle.NUMERIC,
                    ParameterStyle.POSITIONAL_PYFORMAT,
                }:
                    name = f"param_{i}"
                else:
                    name = param.name or f"param_{i}"
                placeholder = f":{name}"
            elif target_style == ParameterStyle.NAMED_PYFORMAT:
                if param.style in {
                    ParameterStyle.POSITIONAL_COLON,
                    ParameterStyle.QMARK,
                    ParameterStyle.NUMERIC,
                    ParameterStyle.POSITIONAL_PYFORMAT,
                }:
                    name = f"param_{i}"
                else:
                    name = param.name or f"param_{i}"
                placeholder = f"%({name})s"
            elif target_style == ParameterStyle.NAMED_AT:
                if param.style in {
                    ParameterStyle.POSITIONAL_COLON,
                    ParameterStyle.QMARK,
                    ParameterStyle.NUMERIC,
                    ParameterStyle.POSITIONAL_PYFORMAT,
                }:
                    name = f"param_{i}"
                else:
                    name = param.name or f"param_{i}"
                placeholder = f"@{name}"
            elif target_style == ParameterStyle.NAMED_DOLLAR:
                if param.style in {
                    ParameterStyle.POSITIONAL_COLON,
                    ParameterStyle.QMARK,
                    ParameterStyle.NUMERIC,
                    ParameterStyle.POSITIONAL_PYFORMAT,
                }:
                    name = f"param_{i}"
                else:
                    name = param.name or f"param_{i}"
                placeholder = f"${name}"
            elif target_style == ParameterStyle.POSITIONAL_COLON:
                placeholder = f":{i + 1}"
            else:
                placeholder = param.placeholder_text

            result_parts.append(placeholder)
            current_pos = param.position + len(param.placeholder_text)

        result_parts.append(sql[current_pos:])

        return "".join(result_parts)

    def needs_conversion(self, parameter_info: list[ParameterInfo], target_style: ParameterStyle) -> bool:
        """Check if parameter style conversion is needed.

        Args:
            parameter_info: List of parameter information
            target_style: Target parameter style

        Returns:
            True if conversion is needed
        """
        if not parameter_info:
            return False

        detected_styles = {p.style for p in parameter_info}

        # Special handling for Oracle numeric parameters
        if target_style == ParameterStyle.NAMED_COLON and ParameterStyle.POSITIONAL_COLON in detected_styles:
            # :1, :2 are valid for named style in Oracle
            return False

        return target_style not in detected_styles

    def _convert_sql_placeholders(
        self, sql: str, target_style: ParameterStyle, parameter_info: Optional[list[ParameterInfo]] = None
    ) -> str:
        """Convert SQL placeholders to target style (alias for convert_placeholders).

        This method exists for backward compatibility with existing code.
        """
        return self.convert_placeholders(sql, target_style, parameter_info)

    def wrap_parameters_with_types(
        self, params: Any, param_info: list[ParameterInfo], literals_parameterized: bool = False
    ) -> Any:
        """Wrap parameters with TypedParameter when type information is needed.

        Args:
            params: The parameters to wrap
            param_info: Parameter information from SQL parsing
            literals_parameterized: Whether literals were parameterized

        Returns:
            Parameters with type information preserved
        """
        # If parameters are already TypedParameter instances, return as-is
        if isinstance(params, TypedParameter):
            return params

        # If we have a dict of parameters
        if isinstance(params, dict):
            # Check if any values are already TypedParameter
            if any(isinstance(v, TypedParameter) for v in params.values()):
                return params

            # For now, return params as-is
            # In the future, we could enhance this to detect types and wrap them
            return params

        # If we have a list/tuple of parameters
        if isinstance(params, (list, tuple)):
            # Check if any values are already TypedParameter
            if any(isinstance(v, TypedParameter) for v in params):
                return params

            # For now, return params as-is
            return params

        # Single parameter value
        return params

    def _transform_sql_for_parsing(
        self, sql: str, param_info: list[ParameterInfo]
    ) -> tuple[str, dict[str, ParameterInfo]]:
        """Transform SQL to use consistent named parameters for parsing.

        This transforms all parameter styles to named colon style (:name) for
        consistent parsing with SQLGlot.

        Args:
            sql: SQL string with mixed parameter styles
            param_info: List of parameter information

        Returns:
            Tuple of (transformed_sql, placeholder_mapping)
        """
        if not param_info:
            return sql, {}

        # Convert all parameters to named style for parsing
        result_parts = []
        current_pos = 0
        placeholder_map = {}

        for i, param in enumerate(param_info):
            # Add SQL up to this parameter
            result_parts.append(sql[current_pos : param.position])

            # Generate unique parameter name
            param_name = f"param_{i}"
            placeholder = f":{param_name}"

            # Store mapping
            placeholder_map[param_name] = param

            result_parts.append(placeholder)
            current_pos = param.position + len(param.placeholder_text)

        # Add remaining SQL
        result_parts.append(sql[current_pos:])

        return "".join(result_parts), placeholder_map

    def merge_parameters(
        self, parameters: Optional[Any], args: Optional[list[Any]], kwargs: Optional[dict[str, Any]]
    ) -> Any:
        """Merge parameters from different sources with precedence rules.

        Args:
            parameters: Primary parameters (takes precedence)
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Merged parameters
        """
        # Parameters takes precedence
        if parameters is not None:
            return parameters

        # Then kwargs
        if kwargs:
            return kwargs

        # Then args
        if args:
            return args

        # Nothing provided
        return None

    def convert_parameters(
        self,
        sql: str,
        parameters: Optional[Any],
        args: Optional[list[Any]],
        kwargs: Optional[dict[str, Any]],
        validate: bool = True,
    ) -> ConvertedParameters:
        """Convert parameters to match SQL parameter style.

        Args:
            sql: SQL string with parameters
            parameters: Primary parameters
            args: Positional arguments
            kwargs: Keyword arguments
            validate: Whether to validate parameters

        Returns:
            ConvertedParameters with transformed SQL and parameters
        """
        # Extract parameter info from SQL
        param_info = self.validator.extract_parameters(sql)

        # Merge parameters
        merged_params = self.merge_parameters(parameters, args[0] if args else None, kwargs)

        # Validate if requested
        if validate and param_info:
            self.validator.validate_parameters(param_info, merged_params, sql)

        # Return result
        return ConvertedParameters(
            transformed_sql=sql,
            parameter_info=param_info,
            merged_parameters=merged_params,
            conversion_state=ParameterStyleConversionState(),
        )

    def _merge_mixed_parameters(
        self, param_info: list[ParameterInfo], args: Optional[list[Any]], kwargs: Optional[dict[str, Any]]
    ) -> dict[str, Any]:
        """Merge mixed positional and named parameters into a single dict.

        Args:
            param_info: Parameter information from SQL
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Dictionary with all parameters merged
        """
        result = {}

        # Add keyword arguments first
        if kwargs:
            result.update(kwargs)

        # Then add positional arguments with generated names
        if args:
            for i, (p_info, value) in enumerate(zip(param_info, args)):
                if p_info.style in {ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NUMERIC}:
                    # For positional parameters, use generated name
                    result[f"arg_{i}"] = value

        return result
