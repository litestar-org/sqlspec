"""ADBC-specific pipeline steps for SQL processing."""

from typing import Any

import sqlglot.expressions as exp

from sqlspec.parameters import TypedParameter
from sqlspec.statement.pipeline import SQLTransformContext
from sqlspec.utils.logging import get_logger

logger = get_logger(__name__)

__all__ = ("adbc_null_transform_step",)


def _is_null_param(param: Any) -> bool:
    """Check if a parameter is NULL."""
    if param is None:
        return True
    return bool(isinstance(param, TypedParameter) and param.value is None)


def _identify_null_parameters(params: Any) -> "tuple[list[int], list[str] | None] | None":
    """Identify NULL parameters and return indices and keys."""
    if isinstance(params, dict):
        param_keys = list(params.keys())
        null_param_indices = [idx for idx, key in enumerate(param_keys) if _is_null_param(params[key])]
        return (null_param_indices, param_keys) if null_param_indices else None
    if isinstance(params, (list, tuple)):
        null_param_indices = [i for i, p in enumerate(params) if _is_null_param(p)]
        return (null_param_indices, None) if null_param_indices else None
    return None


def _transform_null_node(
    node: exp.Expression, null_param_indices: "list[int]", param_keys: "list[str] | None"
) -> exp.Expression:
    """Transform a node if it references a NULL parameter."""
    if isinstance(node, exp.Placeholder):
        return _handle_placeholder_node(node, null_param_indices, param_keys)
    if isinstance(node, exp.Parameter):
        return _handle_parameter_node(node, null_param_indices)
    return node


def _handle_placeholder_node(
    node: exp.Placeholder, null_param_indices: "list[int]", param_keys: "list[str] | None"
) -> exp.Expression:
    """Handle placeholder nodes."""
    placeholder_name = node.this
    if param_keys and placeholder_name in param_keys:
        idx = param_keys.index(placeholder_name)
        if idx in null_param_indices:
            return exp.Null()
    return node


def _handle_parameter_node(node: exp.Parameter, null_param_indices: "list[int]") -> exp.Expression:
    """Handle parameter nodes."""
    param_val = node.this
    if isinstance(param_val, exp.Literal) and param_val.is_int:
        return _handle_numeric_parameter(param_val, null_param_indices)
    if isinstance(param_val, str) and param_val.startswith(("$", "@", ":")):
        return _handle_string_parameter(param_val, null_param_indices)
    return node


def _handle_numeric_parameter(param_val: exp.Literal, null_param_indices: "list[int]") -> exp.Expression:
    """Handle numeric parameters like $1, $2."""
    param_idx = int(param_val.this) - 1
    if param_idx in null_param_indices:
        return exp.Null()
    nulls_before = sum(1 for idx in null_param_indices if idx < param_idx)
    new_idx = param_idx - nulls_before + 1
    return exp.Parameter(this=str(new_idx))


def _handle_string_parameter(param_val: str, null_param_indices: "list[int]") -> exp.Expression:
    """Handle string parameter references."""
    try:
        param_idx = int(param_val[1:]) - 1
        if param_idx in null_param_indices:
            return exp.Null()
        nulls_before = sum(1 for idx in null_param_indices if idx < param_idx)
        new_idx = param_idx - nulls_before + 1
        return exp.Parameter(this=str(new_idx))
    except (ValueError, IndexError):
        pass
    return exp.Parameter(this=param_val)


def _update_parameters(
    context: SQLTransformContext, null_param_indices: "list[int]", param_keys: "list[str] | None"
) -> None:
    """Update context parameters by removing NULL values."""
    if isinstance(context.parameters, dict):
        _update_dict_parameters(context, null_param_indices, param_keys)
    else:
        _update_sequence_parameters(context, null_param_indices)


def _update_dict_parameters(
    context: SQLTransformContext, null_param_indices: "list[int]", param_keys: "list[str] | None"
) -> None:
    """Update dictionary parameters."""
    if not param_keys or not isinstance(context.parameters, dict):
        return

    null_keys = [param_keys[idx] for idx in null_param_indices]

    if all(key.isdigit() for key in param_keys):
        # Renumber numeric keys
        new_params = {}
        new_idx = 1
        for idx, key in enumerate(param_keys):
            if idx not in null_param_indices:
                new_params[str(new_idx)] = context.parameters[key]
                new_idx += 1
        context.parameters = new_params
    else:
        # Remove NULL keys
        for key in null_keys:
            del context.parameters[key]


def _update_sequence_parameters(context: SQLTransformContext, null_param_indices: "list[int]") -> None:
    """Update sequence parameters."""
    original_params = list(context.parameters)
    new_params = [p for i, p in enumerate(original_params) if i not in null_param_indices]

    if isinstance(context.parameters, tuple):
        context.parameters = tuple(new_params)
    else:
        context.parameters = new_params


def _update_metadata(context: SQLTransformContext, null_param_indices: "list[int]") -> None:
    """Update context metadata."""
    context.metadata["adbc_null_transform_applied"] = True
    context.metadata["null_parameter_count"] = len(null_param_indices)

    param_count = len(context.parameters) if isinstance(context.parameters, (dict, list, tuple)) else 0
    context.metadata["all_params_were_null"] = param_count == 0


def adbc_null_transform_step(context: SQLTransformContext) -> SQLTransformContext:
    """Transform NULL parameters for ADBC PostgreSQL driver.

    ADBC PostgreSQL driver cannot handle NULL values in parameter arrays,
    so we need to replace Parameter nodes with Null nodes for NULL values
    and remove them from the parameters.

    This must run BEFORE parameterize_literals_step to ensure we see
    the actual NULL parameters before they get parameterized.
    """
    params = context.parameters

    if not params:
        return context

    null_param_data = _identify_null_parameters(params)
    if not null_param_data:
        return context

    null_param_indices, param_keys = null_param_data

    # Transform the expression tree
    context.current_expression = context.current_expression.transform(
        lambda node: _transform_null_node(node, null_param_indices, param_keys)
    )

    # Remove NULL parameters and update context
    _update_parameters(context, null_param_indices, param_keys)
    _update_metadata(context, null_param_indices)

    return context
