"""AST transformer helpers for parameter processing."""

import bisect
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final, cast

from mypy_extensions import mypyc_attr
from sqlglot import exp as _exp

import sqlspec.exceptions
from sqlspec.core.parameters._alignment import (
    collect_null_parameter_ordinals,
    looks_like_execute_many,
    normalize_parameter_key,
    validate_parameter_alignment,
)
from sqlspec.core.parameters._types import (
    _NAMED_STYLES,
    _POSITIONAL_STYLES,
    ConvertedParameters,
    ParameterMapping,
    ParameterPayload,
    ParameterProfile,
)
from sqlspec.utils.type_guards import get_value_attribute

__all__ = (
    "build_literal_inlining_transform",
    "build_null_pruning_transform",
    "replace_null_parameters_with_literals",
    "replace_placeholders_with_literals",
)


_MISSING_PARAMETER: Final = object()


@mypyc_attr(allow_interpreted_subclasses=False)
class _NullPruningTransform:
    __slots__ = ("_dialect",)

    def __init__(self, dialect: str) -> None:
        self._dialect = dialect

    def __call__(
        self,
        expression: Any,
        parameters: "ParameterPayload",
        parameter_profile: "ParameterProfile",
        is_many: bool = False,
    ) -> "tuple[Any, ConvertedParameters]":
        return replace_null_parameters_with_literals(
            expression, parameters, dialect=self._dialect, parameter_profile=parameter_profile, is_many=is_many
        )


@mypyc_attr(allow_interpreted_subclasses=False)
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


@mypyc_attr(allow_interpreted_subclasses=False)
class _NullPlaceholderTransformer:
    __slots__ = ("_null_names", "_null_positions", "_qmark_position", "_sorted_null_positions")

    def __init__(self, null_positions: "set[int]", sorted_null_positions: "list[int]", null_names: "set[str]") -> None:
        self._null_positions = null_positions
        self._sorted_null_positions = sorted_null_positions
        self._null_names = null_names
        self._qmark_position = 0

    def __call__(self, node: Any) -> Any:
        if isinstance(node, _exp.Placeholder):
            placeholder_value = node.this
            if placeholder_value is None:
                current_position = self._qmark_position
                self._qmark_position += 1
                if current_position in self._null_positions:
                    return _exp.Null()
                return node

            normalized_text = str(placeholder_value).lstrip("$")
            if normalized_text.isdigit():
                param_index = int(normalized_text) - 1
                if param_index in self._null_positions:
                    return _exp.Null()
                shift = bisect.bisect_left(self._sorted_null_positions, param_index)
                new_param_num = param_index - shift + 1
                return _exp.Placeholder(this=f"${new_param_num}")
            return node

        if isinstance(node, _exp.Parameter) and node.this is not None:
            parameter_text = node.this.name if isinstance(node.this, _exp.Var) else str(node.this)
            normalized_text = parameter_text.lstrip("@:$")
            if normalized_text.isdigit():
                param_index = int(normalized_text) - 1
                if param_index in self._null_positions:
                    return _exp.Null()
                shift = bisect.bisect_left(self._sorted_null_positions, param_index)
                new_param_num = param_index - shift + 1
                return _exp.Parameter(this=str(new_param_num))
            if normalized_text in self._null_names:
                return _exp.Null()
            return node

        return node


@mypyc_attr(allow_interpreted_subclasses=False)
class _PlaceholderLiteralTransformer:
    __slots__ = ("_is_mapping", "_is_sequence", "_json_serializer", "_parameters", "_placeholder_index")

    def __init__(self, parameters: "ParameterPayload", json_serializer: "Callable[[Any], str]") -> None:
        self._parameters = parameters
        self._json_serializer = json_serializer
        self._placeholder_index = 0
        self._is_mapping = isinstance(parameters, Mapping)
        self._is_sequence = isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray))

    def _resolve_mapping_value(self, param_name: str, payload: "ParameterMapping") -> object:
        candidate_names = (param_name, f"@{param_name}", f":{param_name}", f"${param_name}", f"param_{param_name}")
        for candidate in candidate_names:
            if candidate in payload:
                return cast("object", get_value_attribute(payload[candidate]))
        normalized = param_name.lstrip("@:$")
        if normalized in payload:
            return cast("object", get_value_attribute(payload[normalized]))
        return _MISSING_PARAMETER

    def __call__(self, node: Any) -> Any:
        parameters = self._parameters
        if isinstance(node, _exp.Placeholder) and self._is_sequence:
            sequence_parameters = cast("Sequence[Any]", parameters)
            current_index = self._placeholder_index
            self._placeholder_index += 1
            if current_index < len(sequence_parameters):
                literal_value = get_value_attribute(sequence_parameters[current_index])
                return _create_literal_expression(literal_value, self._json_serializer)
            return node

        if isinstance(node, _exp.Parameter):
            param_name = str(node.this) if node.this is not None else ""

            if self._is_mapping:
                resolved_value = self._resolve_mapping_value(param_name, cast("ParameterMapping", parameters))
                if resolved_value is not _MISSING_PARAMETER:
                    return _create_literal_expression(resolved_value, self._json_serializer)
                return node

            if self._is_sequence:
                sequence_parameters = cast("Sequence[Any]", parameters)
                name = param_name
                try:
                    if name.startswith("param_"):
                        index_value = int(name[6:])
                        if 0 <= index_value < len(sequence_parameters):
                            literal_value = get_value_attribute(sequence_parameters[index_value])
                            return _create_literal_expression(literal_value, self._json_serializer)
                    if name.isdigit():
                        index_value = int(name)
                        if 0 <= index_value < len(sequence_parameters):
                            literal_value = get_value_attribute(sequence_parameters[index_value])
                            return _create_literal_expression(literal_value, self._json_serializer)
                except (ValueError, AttributeError):
                    return node
            return node

        return node


def build_null_pruning_transform(
    *, dialect: str = "postgres"
) -> "Callable[[Any, ParameterPayload, ParameterProfile, bool], tuple[Any, ConvertedParameters]]":
    """Return a callable that prunes NULL placeholders from an expression."""
    return _NullPruningTransform(dialect)


def build_literal_inlining_transform(
    *, json_serializer: "Callable[[Any], str]"
) -> "Callable[[Any, ParameterPayload, ParameterProfile], tuple[Any, object]]":
    """Return a callable that replaces placeholders with SQL literals."""
    return _LiteralInliningTransform(json_serializer)


def _as_concrete_payload(parameters: "ParameterPayload") -> "ConvertedParameters":
    if parameters is None:
        return None
    if isinstance(parameters, dict):
        return parameters
    if isinstance(parameters, (list, tuple)):
        return parameters
    if isinstance(parameters, Mapping):
        return dict(parameters)
    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
        return list(parameters)
    return None


def replace_null_parameters_with_literals(
    expression: Any,
    parameters: "ParameterPayload",
    *,
    dialect: str = "postgres",
    parameter_profile: "ParameterProfile | None" = None,
    is_many: bool = False,
) -> "tuple[Any, ConvertedParameters]":
    """Rewrite placeholders representing ``NULL`` values and prune parameters.

    Args:
        expression: SQLGlot expression tree to transform.
        parameters: Parameter payload provided by the caller.
        dialect: SQLGlot dialect for serializing the expression.
        parameter_profile: Parameter profile to reuse for validation.
        is_many: Whether the payload is a batch of parameter sets.

    Returns:
        Tuple containing the transformed expression and updated parameters.
    """
    if not parameters:
        if parameters is None:
            return expression, None
        if isinstance(parameters, dict):
            return expression, parameters
        if isinstance(parameters, (list, tuple)):
            return expression, _as_concrete_payload(parameters)
        return expression, None

    if is_many or looks_like_execute_many(parameters):
        if isinstance(parameters, (dict, list, tuple)):
            return expression, _as_concrete_payload(parameters)
        return expression, None

    if parameter_profile is None:
        msg = "replace_null_parameters_with_literals() requires parameter_profile for non-empty parameters"
        raise sqlspec.exceptions.SQLSpecError(msg)
    validate_parameter_alignment(parameter_profile, parameters)

    null_positions = collect_null_parameter_ordinals(parameters, parameter_profile)
    if not null_positions:
        return expression, _as_concrete_payload(parameters)

    null_names: set[str] = set()
    positional_null_positions: set[int] = set()
    for parameter in parameter_profile.parameters:
        if parameter.ordinal not in null_positions:
            continue
        if parameter.style in _NAMED_STYLES and parameter.name:
            null_names.add(parameter.name)
        if parameter.style in _POSITIONAL_STYLES:
            positional_null_positions.add(parameter.ordinal)

    sorted_null_positions = sorted(positional_null_positions)

    transformer = _NullPlaceholderTransformer(positional_null_positions, sorted_null_positions, null_names)
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
