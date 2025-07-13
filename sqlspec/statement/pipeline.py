"""Single-pass pipeline architecture for SQL statement processing.

This module implements the core pipeline infrastructure that transforms
SQL objects through composable functions in a single pass, replacing
the complex multi-stage processing with a clean, efficient approach.
"""

import operator
from dataclasses import dataclass, field
from typing import Any, Callable, Union

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
    "remove_comments_step",
    "validate_dml_safety_step",
    "validate_parameter_style_step",
    "validate_security_step",
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
    parameters: "Union[dict[str, Any], list[Any], tuple[Any, ...]]" = field(default_factory=dict)
    dialect: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def merged_parameters(self) -> Any:
        """Get parameters in appropriate format for the dialect."""
        if isinstance(self.parameters, dict) and self.dialect in {"mysql", "sqlite"}:
            # Convert to positional list ordered by parameter position in SQL
            # This ensures parameters are ordered as they appear in the SQL query
            from sqlspec.statement.parameters import ParameterValidator

            # Parse SQL to get parameter order
            validator = ParameterValidator()
            param_info = validator.extract_parameters(self.current_expression.sql(dialect=self.dialect))

            if param_info:
                # Order parameters by their position in the SQL
                ordered_keys = [
                    p.name for p in sorted(param_info, key=lambda x: x.position) if p.name and p.name in self.parameters
                ]

                # Add any remaining parameters not found in param_info
                for key in self.parameters:
                    if key not in ordered_keys:
                        ordered_keys.append(key)

                return [self.parameters[k] for k in ordered_keys]
            # Fallback to alphabetical if no param info (preserves old behavior for edge cases)
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
    # If parameters already exist, skip literal parameterization to avoid conflicts
    # This preserves existing parameter styles (named, positional, etc.)
    if context.parameters:
        context.metadata["literals_parameterized"] = False
        context.metadata["parameter_count"] = (
            len(context.parameters) if isinstance(context.parameters, (dict, list, tuple)) else 0
        )
        return context

    # First, collect all literals in SQL order
    literals_in_order: list[tuple[exp.Literal, str]] = []
    sql_before = context.current_expression.sql(dialect=context.dialect)

    # First pass: collect literals
    for node in context.current_expression.walk():
        # Skip literals that shouldn't be parameterized
        if isinstance(node, exp.Literal) and not isinstance(
            node.parent, (exp.Placeholder, exp.Parameter, exp.Limit, exp.Offset, exp.Fetch)
        ):
            # Skip literals that are direct aliases (like 'processed' as status)
            if isinstance(node.parent, exp.Alias) and node.parent.this == node:
                continue
            # Get the SQL position by finding the literal in the SQL string
            literal_sql = node.sql(dialect=context.dialect)
            literals_in_order.append((node, literal_sql))

    # Sort by position in SQL string
    literal_positions = []
    remaining_sql = sql_before
    for literal, literal_sql in literals_in_order:
        pos = remaining_sql.find(literal_sql)
        if pos >= 0:
            literal_positions.append((pos + len(sql_before) - len(remaining_sql), literal, literal_sql))
            remaining_sql = remaining_sql[pos + len(literal_sql) :]

    literal_positions.sort(key=operator.itemgetter(0))

    # Ensure parameters is a dict for parameterization
    if not isinstance(context.parameters, dict):
        context.parameters = {}

    # Create parameter mapping
    param_index = len(context.parameters)
    literal_to_param: dict[tuple[str, str], str] = {}

    for _, literal, literal_sql in literal_positions:
        param_name = f"param_{param_index}"
        # Use SQLGlot's to_py() method to get the proper Python type
        context.parameters[param_name] = literal.to_py()
        # Use literal value and SQL representation as key for more reliable lookup
        literal_key = (str(literal.this), literal_sql)
        literal_to_param[literal_key] = param_name
        param_index += 1

    # Second pass: replace literals with placeholders
    def replace_literal(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Literal) and not isinstance(
            node.parent, (exp.Placeholder, exp.Parameter, exp.Limit, exp.Offset, exp.Fetch)
        ):
            # Skip literals that are direct aliases (like 'processed' as status)
            if isinstance(node.parent, exp.Alias) and node.parent.this == node:
                return node
            literal_key = (str(node.this), node.sql(dialect=context.dialect))
            if literal_key in literal_to_param:
                return exp.Placeholder(this=literal_to_param[literal_key])
        return node

    # Transform the expression tree
    context.current_expression = context.current_expression.transform(replace_literal)

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
                suspicious_null_count = 3
                if null_count > suspicious_null_count:
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


def remove_comments_step(context: SQLTransformContext) -> SQLTransformContext:
    """Remove comments and hints from SQL.

    Strips SQL comments and optimizer hints for cleaner processing.
    """
    # For now, SQLGlot already handles comments during parsing
    # This is a placeholder for future enhancements
    context.metadata["comments_removed"] = True
    return context


def validate_dml_safety_step(context: SQLTransformContext) -> SQLTransformContext:
    """Validate DML operations for safety.

    Checks for potentially dangerous DML operations like
    UPDATE/DELETE without WHERE clauses.
    """
    issues = context.metadata.get("validation_issues", [])

    for node in context.current_expression.walk():
        # Check for UPDATE without WHERE
        if isinstance(node, exp.Update) and not node.args.get("where"):
            issues.append("UPDATE without WHERE clause detected")

        # Check for DELETE without WHERE
        if isinstance(node, exp.Delete) and not node.args.get("where"):
            issues.append("DELETE without WHERE clause detected")

        # Check for TRUNCATE
        if isinstance(node, exp.Command) and str(node).upper().startswith("TRUNCATE"):
            issues.append("TRUNCATE operation detected")

    context.metadata["validation_issues"] = issues
    context.metadata["dml_safety_validated"] = True
    return context


def validate_parameter_style_step(context: SQLTransformContext) -> SQLTransformContext:
    """Validate parameter styles are consistent.

    Ensures all parameters use the same style and are properly formatted.
    """
    # This validation happens during compilation in the new architecture
    # as parameter style conversion is handled at the SQL class level
    context.metadata["parameter_style_validated"] = True
    return context


def validate_security_step(context: SQLTransformContext) -> SQLTransformContext:
    """Extended security validation beyond the basic validate_step.

    Performs additional security checks for injection patterns.
    """
    # The basic validate_step already handles security validation
    # This is here for compatibility with the old architecture
    return validate_step(context)
