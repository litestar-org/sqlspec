"""Parameter style conversion utilities."""

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters._types import (
    ConvertedParameters,
    NamedParameterOutput,
    ParameterInfo,
    ParameterMapping,
    ParameterPayload,
    ParameterSequence,
    ParameterStyle,
    PositionalParameterOutput,
)
from sqlspec.core.parameters._validator import ParameterValidator
from sqlspec.exceptions import SQLSpecError

__all__ = ("ParameterConverter",)


def _placeholder_qmark(_: Any) -> str:
    return "?"


def _placeholder_numeric(index: Any) -> str:
    return f"${int(index) + 1}"


def _placeholder_named_colon(name: Any) -> str:
    return f":{name}"


def _placeholder_positional_colon(index: Any) -> str:
    return f":{int(index) + 1}"


def _placeholder_named_at(name: Any) -> str:
    return f"@{name}"


def _placeholder_named_dollar(name: Any) -> str:
    return f"${name}"


def _placeholder_named_pyformat(name: Any) -> str:
    return f"%({name})s"


def _placeholder_positional_pyformat(_: Any) -> str:
    return "%s"


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterConverter:
    """Parameter style conversion helper."""

    __slots__ = ("_format_converters", "_placeholder_generators", "validator")

    def __init__(self, validator: "ParameterValidator | None" = None) -> None:
        self.validator = validator or ParameterValidator()

        self._format_converters = {
            ParameterStyle.POSITIONAL_COLON: self._convert_to_positional_colon_format,
            ParameterStyle.NAMED_COLON: self._convert_to_named_colon_format,
            ParameterStyle.NAMED_PYFORMAT: self._convert_to_named_pyformat_format,
            ParameterStyle.QMARK: self._convert_to_positional_format,
            ParameterStyle.NUMERIC: self._convert_to_positional_format,
            ParameterStyle.POSITIONAL_PYFORMAT: self._convert_to_positional_format,
            ParameterStyle.NAMED_AT: self._convert_to_named_colon_format,
            ParameterStyle.NAMED_DOLLAR: self._convert_to_named_colon_format,
        }

        self._placeholder_generators: dict[ParameterStyle, Callable[[Any], str]] = {
            ParameterStyle.QMARK: _placeholder_qmark,
            ParameterStyle.NUMERIC: _placeholder_numeric,
            ParameterStyle.NAMED_COLON: _placeholder_named_colon,
            ParameterStyle.POSITIONAL_COLON: _placeholder_positional_colon,
            ParameterStyle.NAMED_AT: _placeholder_named_at,
            ParameterStyle.NAMED_DOLLAR: _placeholder_named_dollar,
            ParameterStyle.NAMED_PYFORMAT: _placeholder_named_pyformat,
            ParameterStyle.POSITIONAL_PYFORMAT: _placeholder_positional_pyformat,
        }

    def normalize_sql_for_parsing(
        self, sql: str, dialect: str | None = None, param_info: "list[ParameterInfo] | None" = None
    ) -> "tuple[str, list[ParameterInfo]]":
        param_info = param_info or self.validator.extract_parameters(sql)

        incompatible_styles = self.validator.get_sqlglot_incompatible_styles(dialect)
        needs_conversion = any(p.style in incompatible_styles for p in param_info)

        if not needs_conversion:
            return sql, param_info

        converted_sql = self._convert_to_sqlglot_compatible(sql, param_info, incompatible_styles)
        return converted_sql, param_info

    def _convert_to_sqlglot_compatible(
        self, sql: str, param_info: "list[ParameterInfo]", incompatible_styles: "set[ParameterStyle]"
    ) -> str:
        converted_sql = sql
        for param in reversed(param_info):
            if param.style in incompatible_styles:
                if (
                    param.style in {ParameterStyle.NAMED_COLON, ParameterStyle.NAMED_AT, ParameterStyle.NAMED_DOLLAR}
                    and param.name
                    and param.name.isidentifier()
                ):
                    placeholder_name = param.name
                else:
                    placeholder_name = f"param_{param.ordinal}"
                canonical_placeholder = f":{placeholder_name}"
                converted_sql = (
                    converted_sql[: param.position]
                    + canonical_placeholder
                    + converted_sql[param.position + len(param.placeholder_text) :]
                )
        return converted_sql

    def convert_placeholder_style(
        self,
        sql: str,
        parameters: "ParameterPayload",
        target_style: "ParameterStyle",
        is_many: bool = False,
        *,
        strict_named_parameters: bool = True,
    ) -> "tuple[str, ConvertedParameters]":
        param_info = self.validator.extract_parameters(sql)

        if target_style == ParameterStyle.STATIC:
            return self._embed_static_parameters(sql, parameters, param_info)

        current_styles = {p.style for p in param_info}
        if len(current_styles) == 1 and target_style in current_styles:
            converted_parameters = self._convert_parameter_format(
                parameters,
                param_info,
                target_style,
                parameters,
                preserve_parameter_format=True,
                is_many=is_many,
                strict_named_parameters=strict_named_parameters,
            )
            return sql, converted_parameters

        converted_sql = self._convert_placeholders_to_style(sql, param_info, target_style)
        converted_parameters = self._convert_parameter_format(
            parameters,
            param_info,
            target_style,
            parameters,
            preserve_parameter_format=True,
            is_many=is_many,
            strict_named_parameters=strict_named_parameters,
        )
        return converted_sql, converted_parameters

    def _convert_placeholders_to_style(
        self, sql: str, param_info: "list[ParameterInfo]", target_style: "ParameterStyle"
    ) -> str:
        generator = self._placeholder_generators.get(target_style)
        if generator is None:
            msg = f"Unsupported target parameter style: {target_style}"
            raise ValueError(msg)

        param_styles = {p.style for p in param_info}
        use_sequential_for_qmark = (
            len(param_styles) == 1 and ParameterStyle.QMARK in param_styles and target_style == ParameterStyle.NUMERIC
        )

        unique_params: dict[str, int] = {}
        for param in param_info:
            param_key = (
                f"{param.placeholder_text}_{param.ordinal}"
                if use_sequential_for_qmark and param.style == ParameterStyle.QMARK
                else param.placeholder_text
            )
            if param_key not in unique_params:
                unique_params[param_key] = len(unique_params)

        converted_sql = sql
        placeholder_text_len_cache: dict[str, int] = {}
        for param in reversed(param_info):
            if param.placeholder_text not in placeholder_text_len_cache:
                placeholder_text_len_cache[param.placeholder_text] = len(param.placeholder_text)
            text_len = placeholder_text_len_cache[param.placeholder_text]

            if target_style in {
                ParameterStyle.QMARK,
                ParameterStyle.NUMERIC,
                ParameterStyle.POSITIONAL_PYFORMAT,
                ParameterStyle.POSITIONAL_COLON,
            }:
                param_key = (
                    f"{param.placeholder_text}_{param.ordinal}"
                    if use_sequential_for_qmark and param.style == ParameterStyle.QMARK
                    else param.placeholder_text
                )
                new_placeholder = generator(unique_params[param_key])
            else:
                param_name = param.name or f"param_{param.ordinal}"
                new_placeholder = generator(param_name)

            converted_sql = (
                converted_sql[: param.position] + new_placeholder + converted_sql[param.position + text_len :]
            )

        return converted_sql

    def _convert_sequence_to_dict(
        self, parameters: "ParameterSequence", param_info: "list[ParameterInfo]"
    ) -> "NamedParameterOutput":
        param_dict: dict[str, Any] = {}
        for i, param in enumerate(param_info):
            if i < len(parameters):
                name = param.name or f"param_{param.ordinal}"
                param_dict[name] = parameters[i]
        return param_dict

    def _extract_param_value_mixed_styles(
        self, param: "ParameterInfo", parameters: "ParameterMapping", param_keys: "list[str]"
    ) -> "tuple[object | None, bool]":
        if param.name and param.name in parameters:
            return parameters[param.name], True
        if param.placeholder_text in parameters:
            return parameters[param.placeholder_text], True

        if (
            param.style == ParameterStyle.NUMERIC
            and param.name
            and param.name.isdigit()
            and param.ordinal < len(param_keys)
        ):
            key_to_use = param_keys[param.ordinal]
            return parameters[key_to_use], True

        if f"param_{param.ordinal}" in parameters:
            return parameters[f"param_{param.ordinal}"], True

        ordinal_key = str(param.ordinal + 1)
        if ordinal_key in parameters:
            return parameters[ordinal_key], True

        try:
            ordered_keys = list(parameters.keys())
        except AttributeError:
            ordered_keys = []
        if ordered_keys and param.ordinal < len(ordered_keys):
            key = ordered_keys[param.ordinal]
            if key in parameters:
                return parameters[key], True

        return None, False

    def _extract_param_value_single_style(
        self, param: "ParameterInfo", parameters: "ParameterMapping"
    ) -> "tuple[object | None, bool]":
        if param.name and param.name in parameters:
            return parameters[param.name], True
        if param.placeholder_text in parameters:
            return parameters[param.placeholder_text], True
        if f"param_{param.ordinal}" in parameters:
            return parameters[f"param_{param.ordinal}"], True

        ordinal_key = str(param.ordinal + 1)
        if ordinal_key in parameters:
            return parameters[ordinal_key], True

        try:
            ordered_keys = list(parameters.keys())
        except AttributeError:
            ordered_keys = []
        if ordered_keys and param.ordinal < len(ordered_keys):
            key = ordered_keys[param.ordinal]
            if key in parameters:
                return parameters[key], True

        return None, False

    def _collect_missing_named_parameters(
        self, param_info: "list[ParameterInfo]", parameters: "ParameterMapping"
    ) -> "list[str]":
        named_styles = {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.NAMED_PYFORMAT,
        }
        missing: list[str] = []
        for param in param_info:
            if param.style not in named_styles or not param.name:
                continue
            if param.name in parameters or param.placeholder_text in parameters:
                continue
            missing.append(param.name)
        return sorted(set(missing))

    def _preserve_original_format(
        self, param_values: "list[Any]", original_parameters: object
    ) -> "PositionalParameterOutput":
        if isinstance(original_parameters, tuple):
            return tuple(param_values)
        if isinstance(original_parameters, list):
            return param_values
        if isinstance(original_parameters, Mapping):
            return tuple(param_values)
        return tuple(param_values)

    def _convert_parameter_format(
        self,
        parameters: "ParameterPayload",
        param_info: "list[ParameterInfo]",
        target_style: "ParameterStyle",
        original_parameters: object | None = None,
        preserve_parameter_format: bool = False,
        is_many: bool = False,
        *,
        strict_named_parameters: bool = True,
    ) -> "ConvertedParameters":
        if not parameters or not param_info:
            # When parameters is falsy, it's either None or empty - return None
            if parameters is None:
                return None
            # For empty containers, convert to concrete type
            if isinstance(parameters, Mapping):
                return dict(parameters)
            if isinstance(parameters, (list, tuple)):
                return list(parameters) if isinstance(parameters, list) else tuple(parameters)
            return None

        if (
            is_many
            and isinstance(parameters, Sequence)
            and not isinstance(parameters, (str, bytes, bytearray))
            and parameters
            and isinstance(parameters[0], Mapping)
        ):
            normalized_sets: list[Any] = [
                self._convert_parameter_format(
                    param_set,
                    param_info,
                    target_style,
                    param_set,
                    preserve_parameter_format,
                    is_many=False,
                    strict_named_parameters=strict_named_parameters,
                )
                if isinstance(param_set, Mapping)
                else param_set
                for param_set in parameters
            ]
            if preserve_parameter_format and isinstance(parameters, tuple):
                return tuple(normalized_sets)
            return normalized_sets

        is_named_style = target_style in {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.NAMED_PYFORMAT,
        }

        if is_named_style:
            if isinstance(parameters, Mapping):
                return dict(parameters)
            if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
                return self._convert_sequence_to_dict(parameters, param_info)

        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return list(parameters) if isinstance(parameters, list) else tuple(parameters)

        elif isinstance(parameters, Mapping):
            if strict_named_parameters:
                missing_names = self._collect_missing_named_parameters(param_info, parameters)
                if missing_names:
                    msg = f"Missing named parameter(s): {', '.join(missing_names)}"
                    raise SQLSpecError(msg)
            param_values: list[Any] = []
            parameter_styles = {p.style for p in param_info}
            has_mixed_styles = len(parameter_styles) > 1

            unique_params: dict[str, Any] = {}
            param_order: list[str] = []

            if has_mixed_styles:
                param_keys = list(parameters.keys())
                for param in param_info:
                    param_key = param.placeholder_text if param.name else f"{param.placeholder_text}_{param.ordinal}"
                    if param_key not in unique_params:
                        value, found = self._extract_param_value_mixed_styles(param, parameters, param_keys)
                        if found:
                            unique_params[param_key] = value
                            param_order.append(param_key)
            else:
                for param in param_info:
                    param_key = param.placeholder_text if param.name else f"{param.placeholder_text}_{param.ordinal}"
                    if param_key not in unique_params:
                        value, found = self._extract_param_value_single_style(param, parameters)
                        if found:
                            unique_params[param_key] = value
                            param_order.append(param_key)

            needs_expansion = target_style in {
                ParameterStyle.QMARK,
                ParameterStyle.POSITIONAL_PYFORMAT,
                ParameterStyle.POSITIONAL_COLON,
            }

            if needs_expansion:
                param_values = []
                for param in param_info:
                    param_key = param.placeholder_text if param.name else f"{param.placeholder_text}_{param.ordinal}"
                    if param_key in unique_params:
                        param_values.append(unique_params[param_key])
            else:
                param_values = [unique_params[param_key] for param_key in param_order]

            if preserve_parameter_format and original_parameters is not None:
                return self._preserve_original_format(param_values, original_parameters)

            return param_values

        # Fallback for non-standard parameters - return None
        return None

    def _embed_static_parameters(
        self, sql: str, parameters: "ParameterPayload", param_info: "list[ParameterInfo]"
    ) -> "tuple[str, None]":
        if not param_info:
            return sql, None

        unique_params: dict[str, int] = {}
        for param in param_info:
            if param.style in {ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT}:
                param_key = f"{param.placeholder_text}_{param.ordinal}"
            elif (param.style == ParameterStyle.NUMERIC and param.name) or param.name:
                param_key = param.placeholder_text
            else:
                param_key = f"{param.placeholder_text}_{param.ordinal}"

            if param_key not in unique_params:
                unique_params[param_key] = len(unique_params)

        static_sql = sql
        for param in reversed(param_info):
            param_value = self._get_parameter_value_with_reuse(parameters, param, unique_params)

            if param_value is None:
                literal = "NULL"
            elif isinstance(param_value, str):
                escaped = param_value.replace("'", "''")
                literal = f"'{escaped}'"
            elif isinstance(param_value, bool):
                literal = "TRUE" if param_value else "FALSE"
            elif isinstance(param_value, (int, float)):
                literal = str(param_value)
            else:
                literal = f"'{param_value!s}'"

            static_sql = (
                static_sql[: param.position] + literal + static_sql[param.position + len(param.placeholder_text) :]
            )

        return static_sql, None

    def _get_parameter_value(self, parameters: "ParameterPayload", param: "ParameterInfo") -> object | None:
        if isinstance(parameters, Mapping):
            if param.name and param.name in parameters:
                return parameters[param.name]
            if f"param_{param.ordinal}" in parameters:
                return parameters[f"param_{param.ordinal}"]
            if str(param.ordinal + 1) in parameters:
                return parameters[str(param.ordinal + 1)]
        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            if param.ordinal < len(parameters):
                return parameters[param.ordinal]

        return None

    def _get_parameter_value_with_reuse(
        self, parameters: "ParameterPayload", param: "ParameterInfo", unique_params: "dict[str, int]"
    ) -> object | None:
        if param.style in {ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT}:
            param_key = f"{param.placeholder_text}_{param.ordinal}"
        elif (param.style == ParameterStyle.NUMERIC and param.name) or param.name:
            param_key = param.placeholder_text
        else:
            param_key = f"{param.placeholder_text}_{param.ordinal}"

        unique_ordinal = unique_params.get(param_key)
        if unique_ordinal is None:
            return None

        if isinstance(parameters, Mapping):
            if param.name and param.name in parameters:
                return parameters[param.name]
            if f"param_{unique_ordinal}" in parameters:
                return parameters[f"param_{unique_ordinal}"]
            if str(unique_ordinal + 1) in parameters:
                return parameters[str(unique_ordinal + 1)]
        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            if unique_ordinal < len(parameters):
                return parameters[unique_ordinal]

        return None

    def _convert_to_positional_format(
        self, parameters: "ParameterPayload", param_info: "list[ParameterInfo]"
    ) -> "ConvertedParameters":
        return self._convert_parameter_format(
            parameters, param_info, ParameterStyle.QMARK, parameters, preserve_parameter_format=False
        )

    def _convert_to_named_colon_format(
        self, parameters: "ParameterPayload", param_info: "list[ParameterInfo]"
    ) -> "ConvertedParameters":
        return self._convert_parameter_format(
            parameters, param_info, ParameterStyle.NAMED_COLON, parameters, preserve_parameter_format=False
        )

    def _convert_to_positional_colon_format(
        self, parameters: "ParameterPayload", param_info: "list[ParameterInfo]"
    ) -> "NamedParameterOutput":
        if isinstance(parameters, Mapping):
            return dict(parameters)

        param_dict: dict[str, Any] = {}
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            for index, value in enumerate(parameters):
                param_dict[str(index + 1)] = value

        return param_dict

    def _convert_to_named_pyformat_format(
        self, parameters: "ParameterPayload", param_info: "list[ParameterInfo]"
    ) -> "ConvertedParameters":
        return self._convert_parameter_format(
            parameters, param_info, ParameterStyle.NAMED_PYFORMAT, parameters, preserve_parameter_format=False
        )
