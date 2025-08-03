"""ADBC NULL parameter handling pipeline steps.

This module implements post-processing pipeline steps that handle NULL parameters
after literal parameterization has converted NULL literals to None values.
"""

from typing import Any

import sqlglot.expressions as exp

from sqlspec.statement.pipeline import SQLTransformContext
from sqlspec.utils.logging import get_logger

logger = get_logger(__name__)

__all__ = ("adbc_null_parameter_pipeline_step",)


def adbc_null_parameter_pipeline_step(context: SQLTransformContext) -> SQLTransformContext:
    """Handle NULL parameters for ADBC drivers to prevent Arrow 'na' type issues.

    This POST-PROCESSING pipeline step prevents Arrow from inferring 'na' type by:
    1. Detecting None parameters created by parameterize_literals_step
    2. Removing None parameters from the parameter list
    3. Replacing corresponding SQL placeholders with NULL literals
    4. Preserving parameter ordering for remaining non-NULL parameters

    CRITICAL: This step must run AFTER parameterize_literals_step because:
    - parameterize_literals_step converts: "NULL" literal -> None parameter
    - This step converts back: None parameter -> "NULL" literal (but in AST)
    - This prevents Arrow from seeing None values entirely

    Example Transformation:
    Input:  SQL: "VALUES ($1, $2)", params: ["John", None]
    Output: SQL: "VALUES ($1, NULL)", params: ["John"]
    """
    if not context.parameters:
        return context

    # Detect NULL parameters and their positions
    null_analysis = _analyze_null_parameters(context.parameters)

    if not null_analysis.has_nulls:
        return context

    # Transform SQL AST to replace NULL placeholders with NULL literals
    modified_expression = _replace_null_placeholders_in_ast(context.current_expression, null_analysis)

    # Remove NULL parameters from parameter list and renumber remaining
    cleaned_parameters = _remove_null_parameters(context.parameters, null_analysis)

    # Update context with modifications
    context.current_expression = modified_expression
    context.parameters = cleaned_parameters

    # Store metadata for debugging and validation
    context.metadata["adbc_null_parameters_removed"] = len(null_analysis.null_positions)
    context.metadata["adbc_null_positions"] = list(null_analysis.null_positions.keys())
    context.metadata["adbc_original_param_count"] = null_analysis.original_count
    context.metadata["adbc_final_param_count"] = len(cleaned_parameters) if cleaned_parameters else 0

    logger.debug(
        "ADBC NULL step: Processed %d -> %d parameters, replaced %d NULLs with literals",
        null_analysis.original_count,
        context.metadata["adbc_final_param_count"],
        len(null_analysis.null_positions),
    )

    return context


class _NullParameterAnalysis:
    """Analysis results for NULL parameter detection and transformation."""

    def __init__(self, parameters: Any) -> None:
        self.original_count = 0
        self.null_positions: dict[int, Any] = {}
        self.has_nulls = False

        if isinstance(parameters, (list, tuple)):
            self.original_count = len(parameters)
            for i, param in enumerate(parameters):
                if param is None:
                    self.null_positions[i] = None
            self.has_nulls = len(self.null_positions) > 0
        elif isinstance(parameters, dict):
            # Handle dict parameters with string keys (e.g., {'1': None, '2': 'value'})
            self.original_count = len(parameters)
            for key, param in parameters.items():
                if param is None:
                    try:
                        # Convert string key to 0-based index (e.g., '1' -> 0, '2' -> 1)
                        if isinstance(key, str) and key.lstrip("$").isdigit():
                            param_num = int(key.lstrip("$"))
                            param_index = param_num - 1  # Convert to 0-based
                            self.null_positions[param_index] = None
                        elif isinstance(key, int):
                            self.null_positions[key] = None
                    except ValueError:
                        # Skip non-numeric keys
                        pass
            self.has_nulls = len(self.null_positions) > 0


def _analyze_null_parameters(parameters: Any) -> _NullParameterAnalysis:
    """Analyze parameters to identify NULL values and their positions.

    Args:
        parameters: Parameter list/dict from SQLTransformContext

    Returns:
        Analysis results with NULL positions and metadata
    """
    return _NullParameterAnalysis(parameters)


def _replace_null_placeholders_in_ast(
    expression: exp.Expression, null_analysis: _NullParameterAnalysis
) -> exp.Expression:
    """Replace NULL parameter placeholders with NULL literals in AST.

    This function transforms the SQL AST to replace parameter placeholders
    that correspond to NULL values with actual NULL literal nodes.

    Args:
        expression: SQLglot AST expression
        null_analysis: Analysis results with NULL parameter positions

    Returns:
        Modified AST with NULL literals replacing NULL parameter placeholders
    """

    def transform_node(node: exp.Expression) -> exp.Expression:
        # Handle PostgreSQL/ADBC $1, $2, etc. style placeholders
        if isinstance(node, exp.Placeholder):
            return _transform_postgres_placeholder(node, null_analysis)

        # Handle other parameter styles if needed in the future
        if isinstance(node, exp.Parameter):
            return _transform_parameter_node(node, null_analysis)

        return node

    return expression.transform(transform_node)


def _transform_postgres_placeholder(node: exp.Placeholder, null_analysis: _NullParameterAnalysis) -> exp.Expression:
    """Transform PostgreSQL-style placeholders ($1, $2, etc.).

    Args:
        node: Placeholder node from AST
        null_analysis: NULL parameter analysis results

    Returns:
        Either NULL literal or renumbered placeholder
    """
    if not (hasattr(node, "this") and isinstance(node.this, str)):
        return node

    try:
        # Extract parameter number (1-based) and convert to 0-based index
        param_str = node.this.lstrip("$")
        param_num = int(param_str)
        param_index = param_num - 1

        if param_index in null_analysis.null_positions:
            # Replace with NULL literal
            return exp.Null()
        # Renumber remaining parameters after NULL removal
        nulls_before = sum(1 for idx in null_analysis.null_positions if idx < param_index)
        new_param_num = param_num - nulls_before
        return exp.Placeholder(this=f"${new_param_num}")

    except (ValueError, AttributeError) as e:
        logger.warning("Failed to parse placeholder %s: %s", node.this, e)
        return node


def _transform_parameter_node(node: exp.Parameter, null_analysis: _NullParameterAnalysis) -> exp.Expression:
    """Transform generic parameter nodes (PostgreSQL @1 style in AST).

    Args:
        node: Parameter node from AST
        null_analysis: NULL parameter analysis results

    Returns:
        Either NULL literal or renumbered parameter
    """
    # Handle Parameter nodes (e.g., @1, @2, etc. in AST representation)
    if not hasattr(node, "this"):
        return node

    try:
        # Extract parameter number from Parameter node
        param_str = str(node.this)
        param_num = int(param_str)
        param_index = param_num - 1  # Convert to 0-based index

        if param_index in null_analysis.null_positions:
            # Replace with NULL literal
            return exp.Null()

        # Renumber remaining parameters after NULL removal
        nulls_before = sum(1 for idx in null_analysis.null_positions if idx < param_index)
        new_param_num = param_num - nulls_before
        return exp.Parameter(this=str(new_param_num))

    except (ValueError, AttributeError):
        return node


def _remove_null_parameters(parameters: Any, null_analysis: _NullParameterAnalysis) -> Any:
    """Remove NULL parameters from parameter list.

    Args:
        parameters: Original parameter list/dict
        null_analysis: Analysis results with NULL positions

    Returns:
        Cleaned parameter list with NULLs removed
    """
    if isinstance(parameters, (list, tuple)):
        # Create new list without NULL parameters, preserving order
        return [param for i, param in enumerate(parameters) if i not in null_analysis.null_positions]
    if isinstance(parameters, dict):
        # Handle dict parameters by removing NULL entries and renumbering remaining parameters
        cleaned_dict = {}
        param_keys = sorted(
            parameters.keys(), key=lambda k: int(k.lstrip("$")) if isinstance(k, str) and k.lstrip("$").isdigit() else 0
        )

        new_param_num = 1
        for key in param_keys:
            if parameters[key] is not None:
                # Keep non-NULL parameters and renumber them
                cleaned_dict[str(new_param_num)] = parameters[key]
                new_param_num += 1

        return cleaned_dict

    return parameters
