"""AST transformer helpers for parameter processing."""

import bisect
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sqlspec.core.parameters._alignment import (
    collect_null_parameter_ordinals,
    looks_like_execute_many,
    normalize_parameter_key,
    validate_parameter_alignment,
)
from sqlspec.core.parameters._types import ParameterProfile
from sqlspec.core.parameters._validator import ParameterValidator

__all__ = (
    "build_literal_inlining_transform",
    "build_null_pruning_transform",
    "replace_null_parameters_with_literals",
    "replace_placeholders_with_literals",
)

_AST_TRANSFORMER_VALIDATOR: "ParameterValidator" = ParameterValidator()


def build_null_pruning_transform(
    *, dialect: str = "postgres", validator: "ParameterValidator | None" = None
) -> "Callable[[Any, Any], tuple[Any, Any]]":
    """Return a callable that prunes NULL placeholders from an expression."""

    def transform(expression: Any, parameters: Any) -> "tuple[Any, Any]":
        return replace_null_parameters_with_literals(expression, parameters, dialect=dialect, validator=validator)

    return transform


def build_literal_inlining_transform(
    *, json_serializer: "Callable[[Any], str]"
) -> "Callable[[Any, Any], tuple[Any, Any]]":
    """Return a callable that replaces placeholders with SQL literals."""

    def transform(expression: Any, parameters: Any) -> "tuple[Any, Any]":
        literal_expression = replace_placeholders_with_literals(expression, parameters, json_serializer=json_serializer)
        return literal_expression, parameters

    return transform


def replace_null_parameters_with_literals(
    expression: Any, parameters: Any, *, dialect: str = "postgres", validator: "ParameterValidator | None" = None
) -> "tuple[Any, Any]":
    """Rewrite placeholders representing ``NULL`` values and prune parameters.

    Args:
        expression: SQLGlot expression tree to transform.
        parameters: Parameter payload provided by the caller.
        dialect: SQLGlot dialect for serializing the expression.
        validator: Optional validator instance for parameter extraction.

    Returns:
        Tuple containing the transformed expression and updated parameters.
    """
    if not parameters:
        return expression, parameters

    if looks_like_execute_many(parameters):
        return expression, parameters

    validator_instance = validator or _AST_TRANSFORMER_VALIDATOR
    parameter_info = validator_instance.extract_parameters(expression.sql(dialect=dialect))
    parameter_profile = ParameterProfile(parameter_info)
    validate_parameter_alignment(parameter_profile, parameters)

    null_positions = collect_null_parameter_ordinals(parameters, parameter_profile)
    if not null_positions:
        return expression, parameters

    sorted_null_positions = sorted(null_positions)

    from sqlglot import exp as _exp  # Imported lazily to avoid module-level dependency

    qmark_position = 0

    def transform_node(node: Any) -> Any:
        nonlocal qmark_position

        if isinstance(node, _exp.Placeholder) and getattr(node, "this", None) is None:
            current_position = qmark_position
            qmark_position += 1
            if current_position in null_positions:
                return _exp.Null()
            return node

        if isinstance(node, _exp.Placeholder) and getattr(node, "this", None) is not None:
            placeholder_text = str(node.this)
            normalized_text = placeholder_text.lstrip("$")
            if normalized_text.isdigit():
                param_index = int(normalized_text) - 1
                if param_index in null_positions:
                    return _exp.Null()
                shift = bisect.bisect_left(sorted_null_positions, param_index)
                new_param_num = param_index - shift + 1
                return _exp.Placeholder(this=f"${new_param_num}")
            return node

        if isinstance(node, _exp.Parameter) and getattr(node, "this", None) is not None:
            parameter_text = str(node.this)
            if parameter_text.isdigit():
                param_index = int(parameter_text) - 1
                if param_index in null_positions:
                    return _exp.Null()
                shift = bisect.bisect_left(sorted_null_positions, param_index)
                new_param_num = param_index - shift + 1
                return _exp.Parameter(this=str(new_param_num))
            return node

        return node

    transformed_expression = expression.transform(transform_node)

    if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
        cleaned_parameters = [value for index, value in enumerate(parameters) if index not in null_positions]
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
        cleaned_parameters = cleaned_dict  # type: ignore[assignment]
    else:
        cleaned_parameters = parameters

    return transformed_expression, cleaned_parameters


def _create_literal_expression(value: Any, json_serializer: "Callable[[Any], str]") -> Any:
    """Create a SQLGlot literal expression for the given value."""
    from sqlglot import exp as _exp

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
    expression: Any, parameters: Any, *, json_serializer: "Callable[[Any], str]"
) -> Any:
    """Replace placeholders in an expression tree with literal values."""
    if not parameters:
        return expression

    from sqlglot import exp as _exp

    placeholder_counter = {"index": 0}

    def resolve_mapping_value(param_name: str, payload: Mapping[str, Any]) -> Any | None:
        candidate_names = (param_name, f"@{param_name}", f":{param_name}", f"${param_name}", f"param_{param_name}")
        for candidate in candidate_names:
            if candidate in payload:
                return getattr(payload[candidate], "value", payload[candidate])
        normalized = param_name.lstrip("@:$")
        if normalized in payload:
            return getattr(payload[normalized], "value", payload[normalized])
        return None

    def transform(node: Any) -> Any:
        if (
            isinstance(node, _exp.Placeholder)
            and isinstance(parameters, Sequence)
            and not isinstance(parameters, (str, bytes, bytearray))
        ):
            current_index = placeholder_counter["index"]
            placeholder_counter["index"] += 1
            if current_index < len(parameters):
                literal_value = getattr(parameters[current_index], "value", parameters[current_index])
                return _create_literal_expression(literal_value, json_serializer)
            return node

        if isinstance(node, _exp.Parameter):
            param_name = str(node.this) if getattr(node, "this", None) is not None else ""

            if isinstance(parameters, Mapping):
                resolved_value = resolve_mapping_value(param_name, parameters)
                if resolved_value is not None:
                    return _create_literal_expression(resolved_value, json_serializer)
                return node

            if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
                name = param_name
                try:
                    if name.startswith("param_"):
                        index_value = int(name[6:])
                        if 0 <= index_value < len(parameters):
                            literal_value = getattr(parameters[index_value], "value", parameters[index_value])
                            return _create_literal_expression(literal_value, json_serializer)
                    if name.isdigit():
                        index_value = int(name)
                        if 0 <= index_value < len(parameters):
                            literal_value = getattr(parameters[index_value], "value", parameters[index_value])
                            return _create_literal_expression(literal_value, json_serializer)
                except (ValueError, AttributeError):
                    return node
            return node

        return node

    return expression.transform(transform)
