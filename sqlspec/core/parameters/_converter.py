"""Parameter style conversion utilities."""

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final

from mypy_extensions import mypyc_attr

from sqlspec.core.parameters._types import (
    _NAMED_STYLES,
    _POSITIONAL_STYLES,
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

_ORDERED_PARAM_INFO_MIN_SIZE = 2
_OCCURRENCE_KEYED_STYLES: Final[frozenset[ParameterStyle]] = frozenset(
    {ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT}
)
_EXPANDING_POSITIONAL_STYLES: Final[frozenset[ParameterStyle]] = frozenset(
    {ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.POSITIONAL_COLON}
)


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


def _ordered_parameter_info(param_info: "list[ParameterInfo]") -> "list[ParameterInfo]":
    if len(param_info) < _ORDERED_PARAM_INFO_MIN_SIZE:
        return param_info

    previous_position = param_info[0].position
    for param in param_info[1:]:
        if param.position < previous_position:
            return sorted(param_info, key=lambda item: item.position)
        previous_position = param.position
    return param_info


def _single_parameter_style(param_info: "list[ParameterInfo]") -> "ParameterStyle | None":
    if not param_info:
        return None

    style = param_info[0].style
    for param in param_info[1:]:
        if param.style != style:
            return None
    return style


def _is_positional_style(style: "ParameterStyle") -> bool:
    return style in _POSITIONAL_STYLES


def _parameter_lookup_key(param: "ParameterInfo") -> str:
    if param.style in _OCCURRENCE_KEYED_STYLES:
        return f"{param.placeholder_text}_{param.ordinal}"
    return param.placeholder_text


def _normalized_named_parameter_name(param: "ParameterInfo") -> str:
    param_name = param.name or f"param_{param.ordinal}"
    if param_name.isdigit():
        return f"param_{param.ordinal}"
    return param_name


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterConverter:
    """Parameter style conversion helper."""

    __slots__ = ("_placeholder_generators", "validator")

    def __init__(self, validator: "ParameterValidator | None" = None) -> None:
        self.validator = validator or ParameterValidator()

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

    def convert_placeholder_style(
        self,
        sql: str,
        parameters: "ParameterPayload",
        target_style: "ParameterStyle",
        is_many: bool = False,
        *,
        strict_named_parameters: bool = True,
        param_info: "list[ParameterInfo] | None" = None,
        precomputed_plan: "tuple[list[ParameterInfo], dict[str, int]] | None" = None,
    ) -> "tuple[str, ConvertedParameters]":
        extracted_param_info = param_info if param_info is not None else self.validator.extract_parameters(sql)

        if target_style == ParameterStyle.STATIC:
            return self._embed_static_parameters(sql, parameters, extracted_param_info)

        current_style = _single_parameter_style(extracted_param_info)
        if current_style is not None and target_style == current_style:
            converted_parameters = self._convert_parameter_format(
                parameters,
                extracted_param_info,
                target_style,
                parameters,
                preserve_parameter_format=True,
                is_many=is_many,
                strict_named_parameters=strict_named_parameters,
            )
            return sql, converted_parameters

        converted_sql = self._convert_placeholders_to_style(sql, extracted_param_info, target_style, precomputed_plan)
        converted_parameters = self._convert_parameter_format(
            parameters,
            extracted_param_info,
            target_style,
            parameters,
            preserve_parameter_format=True,
            is_many=is_many,
            strict_named_parameters=strict_named_parameters,
        )
        return converted_sql, converted_parameters

    def _build_conversion_plan(
        self, param_info: "list[ParameterInfo]", target_style: "ParameterStyle"
    ) -> "tuple[list[ParameterInfo], dict[str, int]]":
        ordered_params = _ordered_parameter_info(param_info)

        unique_params: dict[str, int] = {}
        for param in ordered_params:
            param_key = _parameter_lookup_key(param)
            if param_key not in unique_params:
                unique_params[param_key] = len(unique_params)

        return ordered_params, unique_params

    def _convert_placeholders_to_style(
        self,
        sql: str,
        param_info: "list[ParameterInfo]",
        target_style: "ParameterStyle",
        precomputed_plan: "tuple[list[ParameterInfo], dict[str, int]] | None" = None,
    ) -> str:
        generator = self._placeholder_generators.get(target_style)
        if generator is None:
            msg = f"Unsupported target parameter style: {target_style}"
            raise ValueError(msg)

        if precomputed_plan is not None:
            ordered_params, unique_params = precomputed_plan
        else:
            ordered_params, unique_params = self._build_conversion_plan(param_info, target_style)

        # Build SQL using forward iteration with list join (O(n) vs O(n^2) string slicing)
        segments: list[str] = []
        last_end = 0

        is_positional_style = _is_positional_style(target_style)

        for param in ordered_params:
            # Generate new placeholder based on target style
            if is_positional_style:
                param_key = _parameter_lookup_key(param)
                new_placeholder = generator(unique_params[param_key])
            else:
                param_name = _normalized_named_parameter_name(param)
                new_placeholder = generator(param_name)

            # Append segment before this placeholder and the new placeholder
            segments.extend((sql[last_end : param.position], new_placeholder))
            last_end = param.position + len(param.placeholder_text)

        # Append remaining SQL after last placeholder
        segments.append(sql[last_end:])

        return "".join(segments)

    def convert_parameter_info_style(
        self,
        param_info: "list[ParameterInfo]",
        target_style: "ParameterStyle",
        precomputed_plan: "tuple[list[ParameterInfo], dict[str, int]] | None" = None,
    ) -> "list[ParameterInfo]":
        generator = self._placeholder_generators.get(target_style)
        if generator is None:
            msg = f"Unsupported target parameter style: {target_style}"
            raise ValueError(msg)

        if precomputed_plan is not None:
            ordered_params, unique_params = precomputed_plan
        else:
            ordered_params, unique_params = self._build_conversion_plan(param_info, target_style)
        is_positional_style = _is_positional_style(target_style)
        converted_param_info: list[ParameterInfo] = []
        delta = 0

        for param in ordered_params:
            if is_positional_style:
                converted_index = unique_params[_parameter_lookup_key(param)]
                placeholder_text = generator(converted_index)
                name = None
                if target_style in {ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_COLON}:
                    name = str(converted_index + 1)
            else:
                name = _normalized_named_parameter_name(param)
                placeholder_text = generator(name)

            converted_position = param.position + delta
            converted_param_info.append(
                ParameterInfo(
                    name=name,
                    style=target_style,
                    position=converted_position,
                    ordinal=param.ordinal,
                    placeholder_text=placeholder_text,
                )
            )
            delta += len(placeholder_text) - len(param.placeholder_text)

        return converted_param_info

    def _convert_sequence_to_dict(
        self, parameters: "ParameterSequence", param_info: "list[ParameterInfo]"
    ) -> "NamedParameterOutput":
        param_dict: dict[str, Any] = {}
        for i, param in enumerate(param_info):
            if i < len(parameters):
                name = _normalized_named_parameter_name(param)
                param_dict[name] = parameters[i]
        return param_dict

    def _align_mapping_for_named_style(
        self, parameters: "Mapping[str, Any]", param_info: "list[ParameterInfo]"
    ) -> "NamedParameterOutput":
        """Align a mapping with the placeholder names of a named-style target."""
        expected_names = {_normalized_named_parameter_name(param) for param in param_info}
        if expected_names.issubset(parameters.keys()):
            return dict(parameters)
        return self._convert_sequence_to_dict(list(parameters.values()), param_info)

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
        missing: list[str] = []
        for param in param_info:
            if param.style not in _NAMED_STYLES or not param.name:
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
                for param_set in parameters
            ]
            if preserve_parameter_format and isinstance(parameters, tuple):
                return tuple(normalized_sets)
            return normalized_sets

        is_named_style = target_style in _NAMED_STYLES
        if is_named_style:
            if isinstance(parameters, Mapping):
                return self._align_mapping_for_named_style(parameters, param_info)
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

            needs_expansion = target_style in _EXPANDING_POSITIONAL_STYLES

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
            if param.style in _OCCURRENCE_KEYED_STYLES:
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
        if param.style in _OCCURRENCE_KEYED_STYLES:
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
