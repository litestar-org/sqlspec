"""AST transformer helpers for parameter processing."""

import bisect
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from sqlglot import exp as _exp

from sqlspec.core.parameters._alignment import (
    collect_null_parameter_ordinals,
    looks_like_execute_many,
    normalize_parameter_key,
    validate_parameter_alignment,
)
from sqlspec.core.parameters._types import ConvertedParameters, ParameterMapping, ParameterPayload, ParameterProfile
from sqlspec.core.parameters._validator import ParameterValidator
from sqlspec.utils.type_guards import get_value_attribute

__all__ = (
    "build_literal_inlining_transform",
    "build_null_pruning_transform",
    "replace_null_parameters_with_literals",
    "replace_placeholders_with_literals",
)

_AST_TRANSFORMER_VALIDATOR: "ParameterValidator" = ParameterValidator()


class _NullPruningTransform:
    __slots__ = ("_dialect", "_validator")

    def __init__(self, dialect: str, validator: "ParameterValidator | None") -> None:
        self._dialect = dialect
        self._validator = validator

    def __call__(
        self, expression: Any, parameters: "ParameterPayload", parameter_profile: "ParameterProfile"
    ) -> "tuple[Any, ConvertedParameters]":
        return replace_null_parameters_with_literals(
            expression,
            parameters,
            dialect=self._dialect,
            validator=self._validator,
            parameter_profile=parameter_profile,
        )


class _LiteralInliningTransform:
    __slots__ = ("_json_serializer",)

    def __init__(self, json_serializer: "Callable[[Any], str]") -> None:
        self._json_serializer = json_serializer

    def __call__(
        self, expression: Any, parameters: "ParameterPayload", _parameter_profile: "ParameterProfile"
    ) -> "tuple[Any, object]":
        literal_expression = replace_placeholders_with_literals(
            expression, parameters, json_serializer=self._json_serializer
        )
        return literal_expression, parameters


class _NullPlaceholderTransformer:
    __slots__ = ("_null_positions", "_qmark_position", "_sorted_null_positions")

    def __init__(self, null_positions: "set[int]", sorted_null_positions: "list[int]") -> None:
        self._null_positions = null_positions
        self._sorted_null_positions = sorted_null_positions
        self._qmark_position = 0

    def __call__(self, node: Any) -> Any:
        if isinstance(node, _exp.Placeholder) and node.this is None:
            current_position = self._qmark_position
            self._qmark_position += 1
            if current_position in self._null_positions:
                return _exp.Null()
            return node

        if isinstance(node, _exp.Placeholder) and node.this is not None:
            placeholder_text = str(node.this)
            normalized_text = placeholder_text.lstrip("$")
            if normalized_text.isdigit():
                param_index = int(normalized_text) - 1
                if param_index in self._null_positions:
                    return _exp.Null()
                shift = bisect.bisect_left(self._sorted_null_positions, param_index)
                new_param_num = param_index - shift + 1
                return _exp.Placeholder(this=f"${new_param_num}")
            return node

        if isinstance(node, _exp.Parameter) and node.this is not None:
            parameter_text = str(node.this)
            if parameter_text.isdigit():
                param_index = int(parameter_text) - 1
                if param_index in self._null_positions:
                    return _exp.Null()
                shift = bisect.bisect_left(self._sorted_null_positions, param_index)
                new_param_num = param_index - shift + 1
                return _exp.Parameter(this=str(new_param_num))
            return node

        return node


class _PlaceholderLiteralTransformer:
    __slots__ = ("_json_serializer", "_parameters", "_placeholder_index")

    def __init__(self, parameters: "ParameterPayload", json_serializer: "Callable[[Any], str]") -> None:
        self._parameters = parameters
        self._json_serializer = json_serializer
        self._placeholder_index = 0

    def _resolve_mapping_value(self, param_name: str, payload: "ParameterMapping") -> object | None:
        candidate_names = (param_name, f"@{param_name}", f":{param_name}", f"${param_name}", f"param_{param_name}")
        for candidate in candidate_names:
            if candidate in payload:
                return cast("object", get_value_attribute(payload[candidate]))
        normalized = param_name.lstrip("@:$")
        if normalized in payload:
            return cast("object", get_value_attribute(payload[normalized]))
        return None

    def __call__(self, node: Any) -> Any:
        if (
            isinstance(node, _exp.Placeholder)
            and isinstance(self._parameters, Sequence)
            and not isinstance(self._parameters, (str, bytes, bytearray))
        ):
            current_index = self._placeholder_index
            self._placeholder_index += 1
            if current_index < len(self._parameters):
                literal_value = get_value_attribute(self._parameters[current_index])
                return _create_literal_expression(literal_value, self._json_serializer)
            return node

        if isinstance(node, _exp.Parameter):
            param_name = str(node.this) if node.this is not None else ""

            if isinstance(self._parameters, Mapping):
                resolved_value = self._resolve_mapping_value(param_name, self._parameters)
                if resolved_value is not None:
                    return _create_literal_expression(resolved_value, self._json_serializer)
                return node

            if isinstance(self._parameters, Sequence) and not isinstance(self._parameters, (str, bytes, bytearray)):
                name = param_name
                try:
                    if name.startswith("param_"):
                        index_value = int(name[6:])
                        if 0 <= index_value < len(self._parameters):
                            literal_value = get_value_attribute(self._parameters[index_value])
                            return _create_literal_expression(literal_value, self._json_serializer)
                    if name.isdigit():
                        index_value = int(name)
                        if 0 <= index_value < len(self._parameters):
                            literal_value = get_value_attribute(self._parameters[index_value])
                            return _create_literal_expression(literal_value, self._json_serializer)
                except (ValueError, AttributeError):
                    return node
            return node

        return node


def build_null_pruning_transform(
    *, dialect: str = "postgres", validator: "ParameterValidator | None" = None
) -> "Callable[[Any, ParameterPayload, ParameterProfile], tuple[Any, ConvertedParameters]]":
    """Return a callable that prunes NULL placeholders from an expression."""
    return _NullPruningTransform(dialect, validator)


def build_literal_inlining_transform(
    *, json_serializer: "Callable[[Any], str]"
) -> "Callable[[Any, ParameterPayload, ParameterProfile], tuple[Any, object]]":
    """Return a callable that replaces placeholders with SQL literals."""
    return _LiteralInliningTransform(json_serializer)


def replace_null_parameters_with_literals(
    expression: Any,
    parameters: "ParameterPayload",
    *,
    dialect: str = "postgres",
    validator: "ParameterValidator | None" = None,
    parameter_profile: "ParameterProfile | None" = None,
) -> "tuple[Any, ConvertedParameters]":
    """Rewrite placeholders representing ``NULL`` values and prune parameters.

    Args:
        expression: SQLGlot expression tree to transform.
        parameters: Parameter payload provided by the caller.
        dialect: SQLGlot dialect for serializing the expression.
        validator: Optional validator instance for parameter extraction.
        parameter_profile: Optional parameter profile to reuse for validation.

    Returns:
        Tuple containing the transformed expression and updated parameters.
    """
    if not parameters:
        if parameters is None:
            return expression, None
        if isinstance(parameters, dict):
            return expression, parameters
        if isinstance(parameters, (list, tuple)):
            return expression, list(parameters) if isinstance(parameters, list) else tuple(parameters)
        return expression, None

    if looks_like_execute_many(parameters):
        # For execute_many, convert to concrete type
        if isinstance(parameters, dict):
            return expression, parameters
        if isinstance(parameters, (list, tuple)):
            return expression, list(parameters) if isinstance(parameters, list) else tuple(parameters)
        return expression, None

    validator_instance = validator or _AST_TRANSFORMER_VALIDATOR
    profile = parameter_profile
    if profile is None:
        parameter_info = validator_instance.extract_parameters(expression.sql(dialect=dialect))
        profile = ParameterProfile(parameter_info)
    validate_parameter_alignment(profile, parameters)

    null_positions = collect_null_parameter_ordinals(parameters, profile)
    if not null_positions:
        # Convert to concrete type for return
        if isinstance(parameters, dict):
            return expression, parameters
        if isinstance(parameters, (list, tuple)):
            return expression, list(parameters) if isinstance(parameters, list) else tuple(parameters)
        if isinstance(parameters, Mapping):
            return expression, dict(parameters)
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return expression, list(parameters)
        return expression, None

    sorted_null_positions = sorted(null_positions)

    transformer = _NullPlaceholderTransformer(null_positions, sorted_null_positions)
    transformed_expression = expression.transform(transformer)

    cleaned_parameters: ConvertedParameters
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        cleaned_list = [value for index, value in enumerate(parameters) if index not in null_positions]
        cleaned_parameters = tuple(cleaned_list) if isinstance(parameters, tuple) else cleaned_list
    elif isinstance(parameters, Mapping):
        cleaned_dict: dict[str, Any] = {}
        next_numeric_index = 1

        for key, value in parameters.items():
            if value is None:
                continue
            key_kind, normalized_key = normalize_parameter_key(key)
            if key_kind == "index" and isinstance(normalized_key, int):
                cleaned_dict[str(next_numeric_index)] = value
                next_numeric_index += 1
            else:
                cleaned_dict[str(normalized_key)] = value
        cleaned_parameters = cleaned_dict
    else:
        cleaned_parameters = None

    return transformed_expression, cleaned_parameters


def _create_literal_expression(value: Any, json_serializer: "Callable[[Any], str]") -> Any:
    """Create a SQLGlot literal expression for the given value."""
    if value is None:
        return _exp.Null()
    if isinstance(value, bool):
        return _exp.Boolean(this=value)
    if isinstance(value, (int, float)):
        return _exp.Literal.number(str(value))
    if isinstance(value, str):
        return _exp.Literal.string(value)
    if isinstance(value, (list, tuple)):
        items = [_create_literal_expression(item, json_serializer) for item in value]
        return _exp.Array(expressions=items)
    if isinstance(value, dict):
        json_value = json_serializer(value)
        return _exp.Literal.string(json_value)
    return _exp.Literal.string(str(value))


def replace_placeholders_with_literals(
    expression: Any, parameters: "ParameterPayload", *, json_serializer: "Callable[[Any], str]"
) -> Any:
    """Replace placeholders in an expression tree with literal values."""
    if not parameters:
        return expression

    transformer = _PlaceholderLiteralTransformer(parameters, json_serializer)
    return expression.transform(transformer)
