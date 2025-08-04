"""Single-pass pipeline architecture for SQL statement processing.

This module implements the core pipeline infrastructure that transforms
SQL objects through composable functions in a single pass, replacing
the complex multi-stage processing with a clean, efficient approach.
"""

import operator
from typing import TYPE_CHECKING, Any, Callable, Union

import sqlglot.expressions as exp
from sqlglot.optimizer.normalize import normalize
from sqlglot.optimizer.simplify import simplify

from sqlspec.parameters import ParameterValidator
from sqlspec.parameters.types import TypedParameter
from sqlspec.statement.cache import analysis_cache, get_cache_config
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.statement.sql import StatementConfig

logger = get_logger(__name__)

__all__ = (
    "PipelineStep",
    "SQLTransformContext",
    "compose_pipeline",
    "create_pipeline_from_config",
    "generate_analysis_cache_key",
    "metadata_extraction_step",
    "normalize_step",
    "optimize_step",
    "parameter_analysis_step",
    "parameterize_literals_step",
    "returns_rows_analysis_step",
    "validate_dml_safety_step",
    "validate_step",
)


class SQLTransformContext:
    """Carries state through pipeline execution.

    This context object flows through all pipeline steps, accumulating
    transformations and parameters in a single pass.
    """

    __slots__ = ("current_expression", "dialect", "driver_adapter", "metadata", "original_expression", "parameters")

    def __init__(
        self,
        current_expression: exp.Expression,
        original_expression: exp.Expression,
        parameters: "Union[dict[str, Any], list[Any], tuple[Any, ...], None]" = None,
        dialect: str = "",
        metadata: "dict[str, Any] | None" = None,
        driver_adapter: "Any" = None,
    ) -> None:
        self.current_expression = current_expression
        self.original_expression = original_expression
        self.parameters = parameters if parameters is not None else {}
        self.dialect = dialect
        self.metadata = metadata if metadata is not None else {}
        self.driver_adapter = driver_adapter

    @property
    def merged_parameters(self) -> Any:
        """Get parameters in appropriate format for the dialect.

        If a driver adapter is available, delegates to its parameter
        conversion logic for consistency.
        """
        if self.driver_adapter and hasattr(self.driver_adapter, "_convert_parameters_to_driver_format"):
            sql_str = self.current_expression.sql(dialect=self.dialect)
            return self.driver_adapter._convert_parameters_to_driver_format(sql_str, self.parameters)

        if isinstance(self.parameters, dict) and self.dialect in {"mysql", "sqlite", "duckdb"}:
            validator = ParameterValidator()
            param_info = validator.extract_parameters(self.current_expression.sql(dialect=self.dialect))

            if param_info:
                ordered_keys = [
                    p.name for p in sorted(param_info, key=lambda x: x.position) if p.name and p.name in self.parameters
                ]

                for key in self.parameters:
                    if key not in ordered_keys:
                        ordered_keys.append(key)

                return [self.parameters[k] for k in ordered_keys]
            return [self.parameters[k] for k in sorted(self.parameters.keys())]
        return self.parameters


PipelineStep = Callable[[SQLTransformContext], SQLTransformContext]


def generate_analysis_cache_key(sql: str, statement_config: "StatementConfig") -> str:
    """Generate a cache key that incorporates StatementConfig for analysis results.

    The cache key includes:
    - SQL statement hash
    - Analysis configuration flags
    - Dialect
    - Parameter configuration that affects analysis

    This ensures analysis results are cached per unique configuration.
    """
    import hashlib

    sql_hash = hashlib.sha256(sql.encode()).hexdigest()[:16]

    config_parts = [
        f"analysis:{statement_config.enable_analysis}",
        f"dialect:{statement_config.dialect or 'default'}",
        f"param_style:{statement_config.parameter_config.default_parameter_style.value}",
        f"transformations:{statement_config.enable_transformations}",
        f"validation:{statement_config.enable_validation}",
    ]

    config_signature = hashlib.sha256("|".join(config_parts).encode()).hexdigest()[:8]

    return f"analysis:{sql_hash}:{config_signature}"


def with_analysis_caching(step_func: PipelineStep, step_name: str) -> PipelineStep:
    """Wrap an analysis step with caching functionality.

    Args:
        step_func: The analysis step function to wrap
        step_name: Name of the step for cache key generation

    Returns:
        Wrapped step function that checks cache first
    """

    def cached_step(context: SQLTransformContext) -> SQLTransformContext:
        import hashlib

        cache_config = get_cache_config()
        if not cache_config.analysis_cache_enabled:
            return step_func(context)

        sql_text = context.current_expression.sql(dialect=context.dialect)
        step_cache_key = f"{step_name}:{hashlib.sha256(sql_text.encode()).hexdigest()[:16]}"

        cached_result = analysis_cache.get(step_cache_key)
        if cached_result is not None:
            context.metadata.update(cached_result)
            return context

        metadata_before = dict(context.metadata)
        result_context = step_func(context)

        step_results = {
            key: value
            for key, value in result_context.metadata.items()
            if key not in metadata_before or metadata_before.get(key) != value
        }

        if step_results:
            analysis_cache.set(step_cache_key, step_results)

        return result_context

    return cached_step


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


def create_pipeline_from_config(config: "StatementConfig", driver_adapter: "Any" = None) -> PipelineStep:
    """Create a pipeline based on SQL configuration with enhanced architecture.

    This function creates a pipeline that respects the configuration
    settings and uses the enhanced pre/post processing pipeline steps.

    Args:
        config: SQL configuration object
        driver_adapter: Optional driver adapter for driver-specific behavior

    Returns:
        Composed pipeline function
    """
    steps = config.get_pipeline_steps()
    return compose_pipeline(steps)


def parameterize_literals_step(context: SQLTransformContext) -> SQLTransformContext:
    """Replace literals with placeholders - single AST pass.

    Extracts literal values and replaces them with parameter placeholders,
    storing the values in the context for later binding.
    """
    if context.parameters:
        context.metadata["literals_parameterized"] = False
        context.metadata["parameter_count"] = (
            len(context.parameters) if isinstance(context.parameters, (dict, list, tuple)) else 0
        )
        return context

    literals_in_order: list[tuple[exp.Literal, str]] = []
    sql_before = context.current_expression.sql(dialect=context.dialect)

    for node in context.current_expression.walk():
        if isinstance(node, exp.Literal) and not isinstance(
            node.parent, (exp.Placeholder, exp.Parameter, exp.Limit, exp.Offset, exp.Fetch, exp.WindowSpec)
        ):
            if isinstance(node.parent, exp.Alias) and node.parent.this == node:
                continue
            if isinstance(node.parent, exp.Select) and node in node.parent.expressions:
                continue
            literal_sql = node.sql(dialect=context.dialect)
            literals_in_order.append((node, literal_sql))

    literal_positions = []
    remaining_sql = sql_before
    for literal, literal_sql in literals_in_order:
        pos = remaining_sql.find(literal_sql)
        if pos >= 0:
            literal_positions.append((pos + len(sql_before) - len(remaining_sql), literal, literal_sql))
            remaining_sql = remaining_sql[pos + len(literal_sql) :]

    literal_positions.sort(key=operator.itemgetter(0))

    if not isinstance(context.parameters, dict):
        context.parameters = {}

    param_index = len(context.parameters)
    literal_to_param: dict[tuple[str, str], str] = {}

    for _, literal, literal_sql in literal_positions:
        param_name = f"param_{param_index}"
        python_value = literal.to_py()
        python_type = type(python_value).__name__

        data_type = (
            exp.DataType.build("FLOAT")
            if literal.is_number and "." in str(literal.this)
            else exp.DataType.build("INTEGER")
            if literal.is_number
            else exp.DataType.build("VARCHAR")
        )

        context.parameters[param_name] = TypedParameter(value=python_value, data_type=data_type, type_hint=python_type)
        literal_to_param[str(literal.this), literal_sql] = param_name
        param_index += 1

    def replace_literal(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Literal) and not isinstance(
            node.parent, (exp.Placeholder, exp.Parameter, exp.Limit, exp.Offset, exp.Fetch, exp.WindowSpec)
        ):
            if isinstance(node.parent, exp.Alias) and node.parent.this == node:
                return node
            literal_key = (str(node.this), node.sql(dialect=context.dialect))
            if literal_key in literal_to_param:
                return exp.Placeholder(this=literal_to_param[literal_key])
        return node

    context.current_expression = context.current_expression.transform(replace_literal)

    context.metadata["literals_parameterized"] = True
    context.metadata["parameter_count"] = param_index

    return context


def optimize_step(context: SQLTransformContext) -> SQLTransformContext:
    """Apply sqlglot optimizer.

    Runs SQLGlot's optimization passes to simplify and improve
    the SQL expression for better performance.
    """

    try:
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
    Can leverage driver-specific validation if available.
    """
    issues = []
    warnings = []

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

    if context.driver_adapter and hasattr(context.driver_adapter, "suspicious_functions"):
        suspicious_functions.update(context.driver_adapter.suspicious_functions)

    for node in context.current_expression.walk():
        if isinstance(node, exp.Func):
            func_name = node.name.lower() if node.name else ""
            if func_name in suspicious_functions:
                issues.append(f"Suspicious function detected: {func_name}")

        if isinstance(node, exp.Union) and isinstance(node.expression, exp.Select):
            select_expr = node.expression
            if hasattr(select_expr, "expressions"):
                null_count = sum(1 for expr in select_expr.expressions if isinstance(expr, exp.Null))
                suspicious_null_count = 3
                if null_count > suspicious_null_count:
                    warnings.append("Potential UNION injection pattern detected")

        if isinstance(node, exp.EQ):
            left, right = node.this, node.expression
            if isinstance(left, exp.Literal) and isinstance(right, exp.Literal) and left.this == right.this:
                warnings.append(f"Tautology condition detected: {node.sql()}")

        if isinstance(node, exp.Comment):
            warnings.append("SQL comment detected - potential injection vector")

    context.metadata["validation_issues"] = issues
    context.metadata["validation_warnings"] = warnings
    context.metadata["validated"] = True

    if issues:
        logger.warning("Security validation issues: %s", issues)

    return context


def normalize_step(context: SQLTransformContext) -> SQLTransformContext:
    """Normalize SQL expressions for consistent processing.

    Applies SQLGlot's normalization to ensure consistent formatting
    and structure of SQL expressions across different input styles.
    """
    try:
        context.current_expression = normalize(context.current_expression)
        context.metadata["normalized"] = True
    except Exception as e:
        logger.warning("Normalization failed: %s", e)
        context.metadata["normalized"] = False
        context.metadata["normalization_error"] = str(e)

    return context


def validate_dml_safety_step(context: SQLTransformContext) -> SQLTransformContext:
    """Validate DML operations for safety.

    Checks for potentially dangerous DML operations like
    UPDATE/DELETE without WHERE clauses.
    """
    issues = context.metadata.get("validation_issues", [])

    for node in context.current_expression.walk():
        if isinstance(node, exp.Update) and not node.args.get("where"):
            issues.append("UPDATE without WHERE clause detected")
        if isinstance(node, exp.Delete) and not node.args.get("where"):
            issues.append("DELETE without WHERE clause detected")
        if isinstance(node, exp.Command) and str(node).upper().startswith("TRUNCATE"):
            issues.append("TRUNCATE operation detected")

    context.metadata["validation_issues"] = issues
    context.metadata["dml_safety_validated"] = True
    return context


def metadata_extraction_step(context: SQLTransformContext) -> SQLTransformContext:
    """Extract metadata from SQL expression.

    Analyzes the SQL expression to extract structural metadata such as
    tables, columns, operations, and other useful information for
    query analysis and optimization.
    """
    metadata = context.metadata.setdefault("analysis_metadata", {})

    try:
        tables = set()
        for node in context.current_expression.walk():
            if isinstance(node, exp.Table):
                table_name = str(node.this) if node.this else ""
                if table_name:
                    tables.add(table_name)

        metadata["tables"] = list(tables)

        columns = set()
        for node in context.current_expression.walk():
            if isinstance(node, exp.Column):
                column_name = str(node.this) if node.this else ""
                if column_name and column_name != "*":
                    columns.add(column_name)
            elif isinstance(node, exp.Identifier) and isinstance(node.parent, exp.Schema):
                column_name = str(node.this) if node.this else ""
                if column_name:
                    columns.add(column_name)

        metadata["columns"] = list(columns)

        if isinstance(context.current_expression, exp.Select):
            metadata["operation_type"] = "SELECT"
        elif isinstance(context.current_expression, exp.Insert):
            metadata["operation_type"] = "INSERT"
        elif isinstance(context.current_expression, exp.Update):
            metadata["operation_type"] = "UPDATE"
        elif isinstance(context.current_expression, exp.Delete):
            metadata["operation_type"] = "DELETE"
        elif isinstance(context.current_expression, exp.Anonymous):
            metadata["operation_type"] = "ANONYMOUS"
        else:
            metadata["operation_type"] = "OTHER"

        joins = []
        for node in context.current_expression.walk():
            if isinstance(node, exp.Join):
                join_side = getattr(node, "side", None)
                if join_side:
                    joins.append(join_side.upper())
                else:
                    joins.append("INNER")

        metadata["joins"] = joins

        context.metadata["metadata_extracted"] = True

    except Exception as e:
        logger.warning("Metadata extraction failed: %s", e)
        context.metadata["metadata_extracted"] = False
        context.metadata["metadata_extraction_error"] = str(e)

    return context


def returns_rows_analysis_step(context: SQLTransformContext) -> SQLTransformContext:
    """Analyze whether the SQL expression returns rows.

    Determines if the SQL statement will return a result set that
    can be fetched. This is important for driver behavior and
    result handling optimization.
    """
    try:
        returns_rows = False

        if isinstance(context.current_expression, exp.Select):
            returns_rows = True
        elif isinstance(context.current_expression, (exp.Insert, exp.Update, exp.Delete)):
            returns_rows = bool(context.current_expression.find(exp.Returning))
        elif isinstance(context.current_expression, exp.Anonymous):
            returns_rows = _analyze_anonymous_returns_rows(context.current_expression)
        elif isinstance(context.current_expression, (exp.Show, exp.Describe, exp.Pragma)):
            returns_rows = True
        elif isinstance(context.current_expression, exp.With):
            returns_rows = bool(context.current_expression.find(exp.Select))
        else:
            returns_rows = False

        context.metadata["returns_rows"] = returns_rows
        context.metadata["returns_rows_analyzed"] = True

    except Exception as e:
        logger.warning("Returns rows analysis failed: %s", e)
        context.metadata["returns_rows_analyzed"] = False
        context.metadata["returns_rows_analysis_error"] = str(e)
        context.metadata["returns_rows"] = False

    return context


def parameter_analysis_step(context: SQLTransformContext) -> SQLTransformContext:
    """Analyze parameter usage patterns in the SQL expression.

    Examines the parameters used in the SQL statement to provide
    insights about parameter types, positions, and usage patterns
    for optimization and validation purposes.
    """
    try:
        analysis: dict[str, Any] = {
            "parameter_count": 0,
            "parameter_types": set(),
            "has_named_parameters": False,
            "has_positional_parameters": False,
            "parameter_positions": [],
        }

        if context.parameters:
            if isinstance(context.parameters, dict):
                analysis["parameter_count"] = len(context.parameters)
                analysis["has_named_parameters"] = True
                for value in context.parameters.values():
                    if hasattr(value, "value"):
                        analysis["parameter_types"].add(type(value.value).__name__)
                    else:
                        analysis["parameter_types"].add(type(value).__name__)
            elif isinstance(context.parameters, (list, tuple)):
                analysis["parameter_count"] = len(context.parameters)
                analysis["has_positional_parameters"] = True
                for value in context.parameters:
                    if hasattr(value, "value"):
                        analysis["parameter_types"].add(type(value.value).__name__)
                    else:
                        analysis["parameter_types"].add(type(value).__name__)

        placeholder_count = 0
        for node in context.current_expression.walk():
            if isinstance(node, (exp.Placeholder, exp.Parameter)):
                placeholder_count += 1
                analysis["parameter_positions"].append(str(node))

        analysis["placeholder_count"] = placeholder_count
        analysis["parameter_types"] = list(analysis["parameter_types"])

        context.metadata["parameter_analysis"] = analysis
        context.metadata["parameter_analyzed"] = True

    except Exception as e:
        logger.warning("Parameter analysis failed: %s", e)
        context.metadata["parameter_analyzed"] = False
        context.metadata["parameter_analysis_error"] = str(e)

    return context


def _analyze_anonymous_returns_rows(anonymous_expr: exp.Anonymous) -> bool:
    """Analyze anonymous SQL expression to determine if it returns rows.

    This is a helper function for returns_rows_analysis_step to handle
    anonymous expressions that couldn't be parsed into specific types.
    Uses AST traversal when possible, falls back to text analysis for edge cases.
    """
    if not anonymous_expr or not anonymous_expr.this:
        return False

    if anonymous_expr.find(exp.Select):
        return True

    if anonymous_expr.find(exp.Returning):
        return True

    if (
        anonymous_expr.find(exp.Show)
        or anonymous_expr.find(exp.Describe)
        or anonymous_expr.find(exp.Pragma)
        or anonymous_expr.find(exp.With)
    ):
        return True

    sql_text = str(anonymous_expr.this).strip().upper() if anonymous_expr.this else ""
    if not sql_text:
        return False

    returns_patterns = ["SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "PRAGMA", "WITH", "VALUES"]

    no_returns_patterns = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "TRUNCATE",
        "SET",
        "USE",
        "COMMIT",
        "ROLLBACK",
    ]

    first_word = sql_text.split(maxsplit=1)[0] if sql_text.split() else ""

    if first_word in returns_patterns:
        return True
    if first_word in no_returns_patterns:
        if anonymous_expr.find(exp.Returning):
            return True
        return "RETURNING" in sql_text

    return False
