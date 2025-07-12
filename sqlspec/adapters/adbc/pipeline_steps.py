"""ADBC-specific pipeline steps for SQL processing."""

from typing import Any

import sqlglot.expressions as exp

from sqlspec.statement.parameters import TypedParameter
from sqlspec.statement.pipeline import SQLTransformContext
from sqlspec.utils.logging import get_logger

logger = get_logger(__name__)

__all__ = ("adbc_null_transform_step",)


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

    # Helper to check if a parameter is NULL
    def is_null_param(param: Any) -> bool:
        if param is None:
            return True
        return bool(isinstance(param, TypedParameter) and param.value is None)

    # Handle both dict and sequence parameters
    if isinstance(params, dict):
        # Track which parameters are NULL
        null_param_indices = []
        param_keys = list(params.keys())

        # Check for NULL parameters
        for idx, key in enumerate(param_keys):
            if is_null_param(params[key]):
                null_param_indices.append(idx)
    elif isinstance(params, (list, tuple)):
        # For sequences, track indices directly
        null_param_indices = [i for i, p in enumerate(params) if is_null_param(p)]
        param_keys = None
    else:
        # Unknown parameter type, skip transformation
        return context

    if not null_param_indices:
        # No NULL parameters, nothing to do
        logger.debug("ADBC NULL transform - No NULL parameters found, skipping transformation")
        return context

    def transform_node(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Placeholder):
            placeholder_name = node.this
            if param_keys and placeholder_name in param_keys:
                idx = param_keys.index(placeholder_name)
                if idx in null_param_indices:
                    return exp.Null()
        elif isinstance(node, exp.Parameter):
            # node.this might be a Literal object for numeric parameters like $1
            param_val = node.this
            if isinstance(param_val, exp.Literal) and param_val.is_int:
                # This is a numeric parameter like $1, $2, etc.
                param_idx = int(param_val.this) - 1
                if param_idx in null_param_indices:
                    # Replace with NULL
                    return exp.Null()
                # Renumber the parameter based on how many NULLs came before it
                nulls_before = sum(1 for idx in null_param_indices if idx < param_idx)
                new_idx = param_idx - nulls_before + 1  # Convert back to 1-based
                return exp.Parameter(this=exp.Literal.number(new_idx))
            if isinstance(param_val, str) and param_val.startswith(("$", "@", ":")):
                # This is a string parameter reference
                try:
                    param_idx = int(param_val[1:]) - 1
                    if param_idx in null_param_indices:
                        return exp.Null()
                    # Renumber the parameter
                    nulls_before = sum(1 for idx in null_param_indices if idx < param_idx)
                    new_idx = param_idx - nulls_before + 1
                    return exp.Parameter(this=f"{param_val[0]}{new_idx}")
                except (ValueError, IndexError):
                    pass

        return node

    # Transform the expression tree
    context.current_expression = context.current_expression.transform(transform_node)

    # Remove NULL parameters from the context and renumber remaining ones
    if isinstance(context.parameters, dict):
        null_keys = [param_keys[idx] for idx in null_param_indices]

        # For numeric parameter keys, we need to renumber after removal
        if param_keys and all(key.isdigit() for key in param_keys):
            # Create new parameter dict with renumbered keys
            new_params = {}
            new_idx = 1
            for idx, key in enumerate(param_keys):
                if idx not in null_param_indices:
                    new_params[str(new_idx)] = context.parameters[key]
                    new_idx += 1
            context.parameters = new_params
        else:
            # For non-numeric keys, just remove the NULL ones
            for key in null_keys:
                del context.parameters[key]
    else:
        # For sequences, create new sequence without NULL values
        original_params = list(context.parameters)
        new_params = [p for i, p in enumerate(original_params) if i not in null_param_indices]
        # Preserve the original type (tuple or list)
        if isinstance(context.parameters, tuple):
            context.parameters = tuple(new_params)
        else:
            context.parameters = new_params

    # Update metadata
    context.metadata["adbc_null_transform_applied"] = True
    context.metadata["null_parameter_count"] = len(null_param_indices)

    # Check if all params were null
    if isinstance(context.parameters, dict):
        context.metadata["all_params_were_null"] = len(context.parameters) == 0
    else:
        context.metadata["all_params_were_null"] = len(context.parameters) == 0

    # Debug logging
    logger.debug(f"ADBC NULL transform - Removed {len(null_param_indices)} NULL parameters")
    logger.debug(f"ADBC NULL transform - Remaining parameters: {context.parameters}")

    return context
