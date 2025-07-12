"""ADBC-specific pipeline steps for SQL processing."""

import sqlglot.expressions as exp

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

    # Track which parameters are NULL
    null_param_keys = []

    # Check for NULL parameters
    for key, value in params.items():
        if value is None:
            null_param_keys.append(key)

    if not null_param_keys:
        # No NULL parameters, nothing to do
        return context

    def transform_node(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Placeholder):
            placeholder_name = node.this
            if placeholder_name in null_param_keys:
                return exp.Null()
        elif isinstance(node, exp.Parameter):
            # node.this might be a Literal object for numeric parameters like $1
            param_val = node.this
            if isinstance(param_val, exp.Literal) and param_val.is_int:
                # This is a numeric parameter like $1, $2, etc.
                param_idx = int(param_val.this) - 1
                if 0 <= param_idx < len(params):
                    param_keys = list(params.keys())
                    if param_idx < len(param_keys) and param_keys[param_idx] in null_param_keys:
                        return exp.Null()
            elif isinstance(param_val, str) and param_val.startswith(("$", "@", ":")):
                # This is a string parameter reference
                try:
                    param_idx = int(param_val[1:]) - 1
                    if 0 <= param_idx < len(params):
                        param_keys = list(params.keys())
                        if param_idx < len(param_keys) and param_keys[param_idx] in null_param_keys:
                            return exp.Null()
                except (ValueError, IndexError):
                    pass

        return node

    # Transform the expression tree
    context.current_expression = context.current_expression.transform(transform_node)

    # Remove NULL parameters from the context
    for key in null_param_keys:
        del context.parameters[key]

    # Update metadata
    context.metadata["adbc_null_transform_applied"] = True
    context.metadata["null_parameter_count"] = len(null_param_keys)

    return context
