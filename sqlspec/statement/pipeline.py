"""Single-pass pipeline architecture for SQL statement processing.

This module implements the core pipeline infrastructure that transforms
SQL objects through composable functions in a single pass, replacing
the complex multi-stage processing with a clean, efficient approach.
"""

from dataclasses import dataclass, field
from typing import Any, Callable

import sqlglot.expressions as exp

from sqlspec.utils.logging import get_logger

logger = get_logger(__name__)

__all__ = (
    "PipelineStep",
    "SQLTransformContext",
    "compose_pipeline",
    "normalize_step",
    "optimize_step",
    "parameterize_literals_step",
    "validate_step",
)


@dataclass
class SQLTransformContext:
    """Carries state through pipeline execution.

    This context object flows through all pipeline steps, accumulating
    transformations and parameters in a single pass.
    """

    current_expression: exp.Expression
    original_expression: exp.Expression
    parameters: dict[str, Any] = field(default_factory=dict)
    dialect: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def merged_parameters(self) -> Any:
        """Get parameters in appropriate format for the dialect."""
        if self.dialect in ["mysql", "sqlite"]:
            # Convert to positional list
            return [self.parameters[k] for k in sorted(self.parameters.keys())]
        return self.parameters


PipelineStep = Callable[[SQLTransformContext], SQLTransformContext]


def compose_pipeline(steps: list[PipelineStep]) -> PipelineStep:
    """Compose multiple pipeline steps into single function.

    Args:
        steps: List of pipeline functions to compose

    Returns:
        Single function that applies all steps in sequence
    """

    def composed(context: SQLTransformContext) -> SQLTransformContext:
        for step in steps:
            context = step(context)
        return context

    return composed


def normalize_step(context: SQLTransformContext) -> SQLTransformContext:
    """Normalize parameter styles for sqlglot compatibility.

    Converts various parameter styles to a unified format that
    can be processed by subsequent pipeline steps.
    """
    # For now, we handle normalization during compilation
    # This step is a placeholder for future enhancements
    return context


def parameterize_literals_step(context: SQLTransformContext) -> SQLTransformContext:
    """Replace literals with placeholders - single AST pass.

    Extracts literal values and replaces them with parameter placeholders,
    storing the values in the context for later binding.
    """
    param_index = len(context.parameters)

    def extract_literal(node: exp.Expression) -> exp.Expression:
        nonlocal param_index

        if isinstance(node, exp.Literal):
            # Skip literals that are already parameterized or special values
            if isinstance(node.parent, exp.Placeholder):
                return node

            # Skip literals in LIMIT, OFFSET, and other structural SQL contexts
            parent = node.parent
            if parent and isinstance(parent, (exp.Limit, exp.Offset)):
                return node

            # Skip numeric literals used as column references (e.g., ORDER BY 1)
            if parent and isinstance(parent, exp.Order):
                return node

            # Generate parameter name
            param_name = f"param_{param_index}"
            context.parameters[param_name] = node.this
            param_index += 1

            # Replace with placeholder
            return exp.Placeholder(this=param_name)

        return node

    # Transform the expression tree
    context.current_expression = context.current_expression.transform(extract_literal)

    # Record in metadata
    context.metadata["literals_parameterized"] = True
    context.metadata["parameter_count"] = param_index

    return context


def optimize_step(context: SQLTransformContext) -> SQLTransformContext:
    """Apply sqlglot optimizer.

    Runs SQLGlot's optimization passes to simplify and improve
    the SQL expression for better performance.
    """
    from sqlglot.optimizer.simplify import simplify

    try:
        # Apply simplification
        context.current_expression = simplify(context.current_expression)
        context.metadata["optimized"] = True
    except Exception as e:
        logger.warning("Optimization failed: %s", e)
        context.metadata["optimized"] = False
        context.metadata["optimization_error"] = str(e)

    return context


def validate_step(context: SQLTransformContext) -> SQLTransformContext:
    """Run security and safety validation.

    Performs comprehensive security checks on the SQL expression
    to detect potential injection patterns and unsafe operations.
    """
    issues = []
    warnings = []

    # Define suspicious functions that might indicate security issues
    suspicious_functions = {
        "sleep",
        "benchmark",
        "load_file",
        "outfile",
        "dumpfile",
        "exec",
        "xp_cmdshell",
        "sp_executesql",
    }

    for node in context.current_expression.walk():
        # Check for suspicious functions
        if isinstance(node, exp.Func):
            func_name = node.name.lower() if node.name else ""
            if func_name in suspicious_functions:
                issues.append(f"Suspicious function detected: {func_name}")

        # Check for UNION injection patterns
        if isinstance(node, exp.Union) and isinstance(node.expression, exp.Select):
            select_expr = node.expression
            if hasattr(select_expr, "expressions"):
                null_count = sum(1 for expr in select_expr.expressions if isinstance(expr, exp.Null))
                # Suspicious NULL padding (UNION injection often uses multiple NULLs)
                SUSPICIOUS_NULL_COUNT = 3
                if null_count > SUSPICIOUS_NULL_COUNT:
                    warnings.append("Potential UNION injection pattern detected")

        # Check for tautologies (1=1, 'a'='a', etc.)
        if isinstance(node, exp.EQ):
            left, right = node.this, node.expression
            if isinstance(left, exp.Literal) and isinstance(right, exp.Literal) and left.this == right.this:
                warnings.append(f"Tautology condition detected: {node.sql()}")

        # Check for comment injection
        if isinstance(node, exp.Comment):
            warnings.append("SQL comment detected - potential injection vector")

    # Store validation results
    context.metadata["validation_issues"] = issues
    context.metadata["validation_warnings"] = warnings
    context.metadata["validated"] = True

    # Log critical issues
    if issues:
        logger.warning("Security validation issues: %s", issues)

    return context
