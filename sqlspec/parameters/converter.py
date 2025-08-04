"""Parameter conversion logic for SQL placeholders.

This module handles the conversion between different parameter styles.
"""

from typing import Any, Optional, Union, cast

from sqlspec.parameters.types import (
    SQLGLOT_INCOMPATIBLE_STYLES,
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
        parameter_info = parameter_info or self.validator.extract_parameters(sql)

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

        if target_style == ParameterStyle.NAMED_COLON and ParameterStyle.POSITIONAL_COLON in detected_styles:
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

        Wraps complex types that need special handling by database adapters:
        - Boolean values (need adapter-specific conversion)
        - Decimals (precision/scale handling)
        - Dates/times (format conversion)
        - Large integers (> 32-bit)
        - Collections (arrays, dicts)
        - NULL values

        Simple types (strings, small ints, floats) are not wrapped for performance.

        Args:
            params: The parameters to wrap
            param_info: Parameter information from SQL parsing
            literals_parameterized: Whether literals were parameterized

        Returns:
            Parameters with type information preserved
        """
        if isinstance(params, TypedParameter):
            return params

        if isinstance(params, dict):
            if any(isinstance(v, TypedParameter) for v in params.values()):
                return params
            return {key: self._wrap_single_parameter(value, key) for key, value in params.items()}

        if isinstance(params, (list, tuple)):
            if any(isinstance(v, TypedParameter) for v in params):
                return params

            return [
                self._wrap_single_parameter(
                    value, param_info[i].name if i < len(param_info) and param_info[i].name else None
                )
                for i, value in enumerate(params)
            ]
        return self._wrap_single_parameter(params, None)

    def _wrap_single_parameter(self, value: Any, semantic_name: Optional[str] = None) -> Any:
        """Wrap a single parameter value if it needs type information.

        Args:
            value: The parameter value
            semantic_name: Optional semantic name for the parameter

        Returns:
            The value wrapped in TypedParameter if needed, otherwise the raw value
        """
        from datetime import date, datetime
        from decimal import Decimal

        import sqlglot.expressions as exp

        from sqlspec.parameters.types import MAX_32BIT_INT

        if isinstance(value, TypedParameter):
            return value
        if value is None:
            return TypedParameter(
                value=None, data_type=exp.DataType.build("NULL"), type_hint="null", semantic_name=semantic_name
            )

        if isinstance(value, bool):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("BOOLEAN"), type_hint="boolean", semantic_name=semantic_name
            )

        if isinstance(value, int) and abs(value) > MAX_32BIT_INT:
            return TypedParameter(
                value=value, data_type=exp.DataType.build("BIGINT"), type_hint="bigint", semantic_name=semantic_name
            )

        if isinstance(value, Decimal):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("DECIMAL"), type_hint="decimal", semantic_name=semantic_name
            )

        if isinstance(value, date) and not isinstance(value, datetime):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("DATE"), type_hint="date", semantic_name=semantic_name
            )

        if isinstance(value, datetime):
            return TypedParameter(
                value=value,
                data_type=exp.DataType.build("TIMESTAMP"),
                type_hint="timestamp",
                semantic_name=semantic_name,
            )

        if isinstance(value, (bytes, bytearray)):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("BINARY"), type_hint="binary", semantic_name=semantic_name
            )

        if isinstance(value, (list, tuple)) and not isinstance(value, str):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("ARRAY"), type_hint="array", semantic_name=semantic_name
            )

        if isinstance(value, dict):
            return TypedParameter(
                value=value, data_type=exp.DataType.build("JSON"), type_hint="json", semantic_name=semantic_name
            )

        return value

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

        result_parts = []
        current_pos = 0
        placeholder_map = {}

        for i, param in enumerate(param_info):
            result_parts.append(sql[current_pos : param.position])

            param_name = f"param_{i}"
            placeholder = f":{param_name}"

            placeholder_map[param_name] = param

            result_parts.append(placeholder)
            current_pos = param.position + len(param.placeholder_text)
        result_parts.append(sql[current_pos:])

        return "".join(result_parts), placeholder_map

    def merge_parameters(
        self, parameters: Optional[Any], args: Optional[list[Any]], kwargs: Optional[dict[str, Any]]
    ) -> Any:
        """Merge parameters from different sources with precedence rules.

        Args:
            parameters: Primary parameters (takes precedence)
            args: Positional arguments (list or single element)
            kwargs: Keyword arguments

        Returns:
            Merged parameters
        """
        if parameters is not None:
            return parameters

        if kwargs:
            return kwargs

        if args is not None:
            return args if len(args) != 1 else args[0]
        return None

    def convert_mixed_parameters_to_dict(self, params: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert mixed parameter styles to a consistent dict format.

        This method handles the case where SQL has mixed parameter styles
        (e.g., ? and $1) and ensures each parameter gets a unique key.

        Args:
            params: Parameters in list, tuple, or dict format
            param_info: Parameter information extracted from SQL

        Returns:
            Dictionary with unique keys for each parameter
        """
        if not param_info:
            return {}

        if isinstance(params, dict):
            return params

        if not isinstance(params, (list, tuple)):
            return {"param_0": params} if params is not None else {}

        params_dict: dict[str, Any] = {}
        sorted_params = sorted(param_info, key=lambda p: p.position)

        for i, p_info in enumerate(sorted_params):
            if i < len(params):
                key = p_info.name if p_info.name is not None else f"param_{i}"
                if key in params_dict:
                    key = f"param_{i}"
                params_dict[key] = params[i]

        return params_dict

    def convert_parameters(
        self,
        sql: str,
        parameters: Optional[Any],
        args: Optional[list[Any]],
        kwargs: Optional[dict[str, Any]],
        validate: bool = True,
    ) -> ConvertedParameters:
        """Convert parameters to match SQL parameter style.

        This method implements SQLGlot-incompatible parameter style conversion.
        When SQLGlot-incompatible styles are detected, they are automatically
        converted to named colon style (:param_0, :param_1, etc.) for SQLGlot parsing.

        Args:
            sql: SQL string with parameters
            parameters: Primary parameters
            args: Positional arguments
            kwargs: Keyword arguments
            validate: Whether to validate parameters

        Returns:
            ConvertedParameters with transformed SQL and parameters
        """

        param_info = self.validator.extract_parameters(sql)

        merged_params = self.merge_parameters(parameters, args, kwargs)

        if validate and param_info:
            self.validator.validate_parameters(param_info, merged_params, sql)

        detected_styles = {p.style for p in param_info}
        needs_conversion = any(style in SQLGLOT_INCOMPATIBLE_STYLES for style in detected_styles)

        if needs_conversion:
            transformed_sql, conversion_state = self._convert_to_sqlglot_compatible(sql, param_info, merged_params)
            return ConvertedParameters(
                transformed_sql=transformed_sql,
                parameter_info=self.validator.extract_parameters(transformed_sql),
                merged_parameters=merged_params,
                conversion_state=conversion_state,
            )
        return ConvertedParameters(
            transformed_sql=sql,
            parameter_info=param_info,
            merged_parameters=merged_params,
            conversion_state=ParameterStyleConversionState(was_transformed=False),
        )

    def _convert_to_sqlglot_compatible(
        self, sql: str, param_info: list[ParameterInfo], merged_params: Any
    ) -> tuple[str, ParameterStyleConversionState]:
        """Convert SQL with SQLGlot-incompatible parameters to compatible named colon style.

        Args:
            sql: Original SQL string
            param_info: Parameter information extracted from SQL
            merged_params: Merged parameter values

        Returns:
            Tuple of (transformed_sql, conversion_state)
        """
        if not param_info:
            return sql, ParameterStyleConversionState(was_transformed=False)

        result_parts = []
        current_pos = 0
        placeholder_map: dict[str, str] = {}
        reverse_map: dict[str, str] = {}
        original_styles = list({p.style for p in param_info})

        for i, param in enumerate(param_info):
            result_parts.append(sql[current_pos : param.position])

            new_placeholder = f":param_{i}"
            original_placeholder = param.placeholder_text

            placeholder_map[original_placeholder] = new_placeholder
            reverse_map[new_placeholder] = original_placeholder

            result_parts.append(new_placeholder)
            current_pos = param.position + len(param.placeholder_text)

        result_parts.append(sql[current_pos:])
        return "".join(result_parts), ParameterStyleConversionState(
            was_transformed=True,
            original_styles=original_styles,
            transformation_style=ParameterStyle.NAMED_COLON,
            placeholder_map=cast("dict[str, Union[str, int]]", placeholder_map),
            reverse_map=cast("dict[Union[str, int], str]", reverse_map),
            original_param_info=param_info,
        )

    def convert_placeholder_style(
        self, sql: str, params: Any, target_style: ParameterStyle, is_many: bool = False
    ) -> tuple[str, Any]:
        """Convert SQL and parameters to the requested placeholder style.

        This is a comprehensive parameter style conversion that handles both SQL
        placeholder conversion and parameter data structure conversion.

        Args:
            sql: The SQL string to convert
            params: The parameters to convert
            target_style: Target placeholder style
            is_many: Whether this is for execute_many operation

        Returns:
            Tuple of (converted_sql, converted_params)
        """
        if is_many and isinstance(params, list) and params and isinstance(params[0], (list, tuple)):
            param_info = self.validator.extract_parameters(sql)
            if param_info:
                converted_sql = self.convert_placeholders(sql, target_style, param_info)
                return converted_sql, params
            return sql, params

        param_info = self.validator.extract_parameters(sql)
        if not param_info:
            return sql, params

        if target_style == ParameterStyle.STATIC:
            return self._embed_static_parameters(sql, params, param_info)

        if all(p.style == target_style for p in param_info):
            converted_params = self._convert_parameters_format(params, param_info, target_style)
            return sql, converted_params

        converted_sql = self.convert_placeholders(sql, target_style, param_info)
        converted_params = self._convert_parameters_format(params, param_info, target_style)

        return converted_sql, converted_params

    def _embed_static_parameters(self, sql: str, params: Any, param_info: list[ParameterInfo]) -> tuple[str, None]:
        """Embed parameter values directly into SQL for STATIC style.

        Args:
            sql: The SQL string with placeholders
            params: The parameter values
            param_info: List of parameter information from extraction

        Returns:
            Tuple of (sql_with_embedded_values, None)
        """
        import sqlglot

        param_list: list[Any] = []
        if isinstance(params, dict):
            for p in param_info:
                if p.name and p.name in params:
                    param_list.append(params[p.name])
                elif f"param_{p.ordinal}" in params:
                    param_list.append(params[f"param_{p.ordinal}"])
                elif f"arg_{p.ordinal}" in params:
                    param_list.append(params[f"arg_{p.ordinal}"])
                else:
                    param_list.append(params.get(str(p.ordinal), None))
        elif isinstance(params, (list, tuple)):
            param_list = list(params)
        elif params is not None:
            param_list = [params]

        sorted_params = sorted(param_info, key=lambda p: p.position, reverse=True)

        for p in sorted_params:
            if p.ordinal < len(param_list):
                value = param_list[p.ordinal]

                if hasattr(value, "value"):
                    value = value.value
                if value is None:
                    literal_str = "NULL"
                elif isinstance(value, bool):
                    literal_str = "TRUE" if value else "FALSE"
                elif isinstance(value, str):
                    literal_expr = sqlglot.exp.Literal.string(value)
                    literal_str = literal_expr.sql()
                elif isinstance(value, (int, float)):
                    literal_expr = sqlglot.exp.Literal.number(value)
                    literal_str = literal_expr.sql()
                else:
                    literal_expr = sqlglot.exp.Literal.string(str(value))
                    literal_str = literal_expr.sql()

                start = p.position
                end = start + len(p.placeholder_text)
                sql = sql[:start] + literal_str + sql[end:]

        return sql, None

    def _convert_parameters_format(
        self, params: Any, param_info: list[ParameterInfo], target_style: ParameterStyle
    ) -> Any:
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

    def _convert_to_positional_colon_format(self, params: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert parameters to positional colon format (Oracle :1, :2, :3)."""
        if isinstance(params, dict):
            return params
        if isinstance(params, (list, tuple)):
            return self._convert_list_to_colon_dict(params, param_info)
        if params is not None:
            return self._convert_single_value_to_colon_dict(params, param_info)
        return {}

    def _convert_to_positional_format(self, params: Any, param_info: list[ParameterInfo]) -> list[Any]:
        """Convert parameters to positional format (?, $1, %s)."""
        if isinstance(params, (list, tuple)):
            return list(params)
        if isinstance(params, dict):
            param_list = []

            param_values = list(params.values())

            for i, p in enumerate(param_info):
                if p.name and p.name in params:
                    param_list.append(params[p.name])
                elif f"param_{p.ordinal}" in params:
                    param_list.append(params[f"param_{p.ordinal}"])
                elif str(p.ordinal + 1) in params:
                    param_list.append(params[str(p.ordinal + 1)])
                elif i < len(param_values):
                    param_list.append(param_values[i])
                else:
                    param_list.append(None)
            return param_list
        if params is not None:
            return [params]
        return []

    def _convert_to_named_colon_format(self, params: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert parameters to named colon format (:name, :param_0)."""
        if isinstance(params, dict):
            return params
        if isinstance(params, (list, tuple)):
            result = {}
            for i, value in enumerate(params):
                param_name = None
                if i < len(param_info) and param_info[i].name:
                    param_name = param_info[i].name
                if param_name:
                    result[param_name] = value
                else:
                    result[f"param_{i}"] = value
            return result
        if params is not None:
            param_name = param_info[0].name if param_info and param_info[0].name else "param_0"
            return {param_name: params}
        return {}

    def _convert_to_named_pyformat_format(self, params: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert parameters to named pyformat format (%(name)s, %(param_0)s)."""
        return self._convert_to_named_colon_format(params, param_info)

    def _convert_list_to_colon_dict(
        self, params: "list[Any] | tuple[Any, ...]", param_info: list[ParameterInfo]
    ) -> dict[str, Any]:
        """Convert list/tuple parameters to colon-style dict format."""
        result_dict: dict[str, Any] = {}

        if param_info:
            has_numeric = any(p.style == ParameterStyle.NUMERIC for p in param_info)
            has_other_styles = any(p.style != ParameterStyle.NUMERIC for p in param_info)

            if has_numeric and has_other_styles:
                sorted_params = sorted(param_info, key=lambda p: p.position)
                for i, _ in enumerate(sorted_params):
                    if i < len(params):
                        result_dict[str(i + 1)] = params[i]
                return result_dict

            all_numeric = all(p.name and p.name.isdigit() for p in param_info)
            if all_numeric:
                for i, value in enumerate(params):
                    result_dict[str(i + 1)] = value
            else:
                for i, value in enumerate(params):
                    if i < len(param_info):
                        param_name = param_info[i].name or str(i + 1)
                        result_dict[param_name] = value
                    else:
                        result_dict[str(i + 1)] = value
        else:
            for i, value in enumerate(params):
                result_dict[str(i + 1)] = value

        return result_dict

    def _convert_single_value_to_colon_dict(self, params: Any, param_info: list[ParameterInfo]) -> dict[str, Any]:
        """Convert single value parameter to colon-style dict format."""
        result_dict: dict[str, Any] = {}
        if param_info and param_info[0].name and param_info[0].name.isdigit():
            result_dict[param_info[0].name] = params
        else:
            result_dict["1"] = params
        return result_dict
