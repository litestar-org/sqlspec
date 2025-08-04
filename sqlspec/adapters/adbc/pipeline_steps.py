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
    """Handle NULL parameters for ADBC drivers to prevent Arrow type inference issues.

    Converts None parameters back to NULL literals in the SQL to prevent Arrow
    from inferring 'na' types. Must run after parameterize_literals_step.

    Args:
        context: SQL transformation context with parameters and expression

    Returns:
        Modified context with NULL literals replacing None parameters
    """
    if not context.parameters:
        return context

    null_analysis = _analyze_null_parameters(context.parameters)

    if not null_analysis.has_nulls:
        return context

    modified_expression = _replace_null_placeholders_in_ast(context.current_expression, null_analysis)

    cleaned_parameters = _remove_null_parameters(context.parameters, null_analysis)

    context.current_expression = modified_expression
    context.parameters = cleaned_parameters
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
            self.original_count = len(parameters)
            for key, param in parameters.items():
                if param is None:
                    try:
                        if isinstance(key, str) and key.lstrip("$").isdigit():
                            param_num = int(key.lstrip("$"))
                            param_index = param_num - 1
                            self.null_positions[param_index] = None
                        elif isinstance(key, int):
                            self.null_positions[key] = None
                    except ValueError:
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
        if isinstance(node, exp.Placeholder):
            return _transform_postgres_placeholder(node, null_analysis)

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
        param_str = node.this.lstrip("$")
        param_num = int(param_str)
        param_index = param_num - 1

        if param_index in null_analysis.null_positions:
            return exp.Null()
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
    if not hasattr(node, "this"):
        return node

    try:
        param_str = str(node.this)
        param_num = int(param_str)
        param_index = param_num - 1

        if param_index in null_analysis.null_positions:
            return exp.Null()
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
        return [param for i, param in enumerate(parameters) if i not in null_analysis.null_positions]
    if isinstance(parameters, dict):
        cleaned_dict = {}
        param_keys = sorted(
            parameters.keys(), key=lambda k: int(k.lstrip("$")) if isinstance(k, str) and k.lstrip("$").isdigit() else 0
        )

        new_param_num = 1
        for key in param_keys:
            if parameters[key] is not None:
                cleaned_dict[str(new_param_num)] = parameters[key]
                new_param_num += 1

        return cleaned_dict

    return parameters
