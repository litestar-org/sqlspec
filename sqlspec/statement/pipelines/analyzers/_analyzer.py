"""SQL statement analyzer for extracting metadata and complexity metrics."""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.pipelines.context import SQLProcessingContext
    from sqlspec.statement.sql import SQLConfig

__all__ = (
    "StatementAnalysis",
    "StatementAnalyzer",
)

# Constants for statement analysis
HIGH_SUBQUERY_COUNT_THRESHOLD = 10
"""Threshold for flagging high number of subqueries."""

HIGH_CORRELATED_SUBQUERY_THRESHOLD = 3
"""Threshold for flagging multiple correlated subqueries."""

EXPENSIVE_FUNCTION_THRESHOLD = 5
"""Threshold for flagging multiple expensive functions."""

NESTED_FUNCTION_THRESHOLD = 3
"""Threshold for flagging multiple nested function calls."""

logger = logging.getLogger("sqlspec.statement.analyzers")


@dataclass
class StatementAnalysis:
    """Analysis result for parsed SQL statements."""

    statement_type: str
    """Type of SQL statement (Insert, Select, Update, Delete, etc.)"""
    expression: exp.Expression
    """Parsed SQLGlot expression"""
    table_name: "Optional[str]" = None
    """Primary table name if detected"""
    columns: "list[str]" = field(default_factory=list)
    """Column names if detected"""
    has_returning: bool = False
    """Whether statement has RETURNING clause"""
    is_from_select: bool = False
    """Whether this is an INSERT FROM SELECT pattern"""
    parameters: "dict[str, Any]" = field(default_factory=dict)
    """Extracted parameters from the SQL"""
    tables: "list[str]" = field(default_factory=list)
    """All table names referenced in the query"""
    complexity_score: int = 0
    """Complexity score based on query structure"""
    uses_subqueries: bool = False
    """Whether the query uses subqueries"""
    join_count: int = 0
    """Number of joins in the query"""
    aggregate_functions: "list[str]" = field(default_factory=list)
    """List of aggregate functions used"""

    # Enhanced complexity metrics
    join_types: "dict[str, int]" = field(default_factory=dict)
    """Types and counts of joins"""
    max_subquery_depth: int = 0
    """Maximum subquery nesting depth"""
    correlated_subquery_count: int = 0
    """Number of correlated subqueries"""
    function_count: int = 0
    """Total number of function calls"""
    where_condition_count: int = 0
    """Number of WHERE conditions"""
    potential_cartesian_products: int = 0
    """Number of potential Cartesian products detected"""
    complexity_warnings: "list[str]" = field(default_factory=list)
    """Warnings about query complexity"""
    complexity_issues: "list[str]" = field(default_factory=list)
    """Issues with query complexity"""


class StatementAnalyzer(ProcessorProtocol[exp.Expression]):
    """SQL statement analyzer that extracts metadata and insights from SQL statements.

    This processor analyzes SQL expressions to extract useful metadata without
    modifying the SQL itself. It can be used in pipelines to gather insights
    about query complexity, table usage, etc.
    """

    def __init__(
        self,
        cache_size: int = 1000,
        max_join_count: int = 10,
        max_subquery_depth: int = 3,
        max_function_calls: int = 20,
        max_where_conditions: int = 15,
    ) -> None:
        """Initialize the analyzer.

        Args:
            cache_size: Maximum number of parsed expressions to cache.
            max_join_count: Maximum allowed joins before flagging.
            max_subquery_depth: Maximum allowed subquery nesting depth.
            max_function_calls: Maximum allowed function calls.
            max_where_conditions: Maximum allowed WHERE conditions.
        """
        self.cache_size = cache_size
        self.max_join_count = max_join_count
        self.max_subquery_depth = max_subquery_depth
        self.max_function_calls = max_function_calls
        self.max_where_conditions = max_where_conditions
        self._parse_cache: dict[tuple[str, Optional[str]], exp.Expression] = {}
        self._analysis_cache: dict[str, StatementAnalysis] = {}

    def process(self, context: "SQLProcessingContext") -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Process the SQL expression to extract analysis metadata and store it in the context."""

        if not context.config.enable_analysis:
            if context.current_expression is None:
                return exp.Placeholder(), None
            return context.current_expression, None

        if context.current_expression is None:
            logger.warning(
                "StatementAnalyzer.process called with no current_expression in context when analysis is enabled."
            )
            return exp.Placeholder(), None

        analysis_result_obj = self.analyze_expression(
            context.current_expression, context.dialect, context.config, context.validation_result
        )
        context.analysis_result = analysis_result_obj
        return context.current_expression, None

    def analyze_statement(self, sql_string: str, dialect: "Optional[DialectType]" = None) -> StatementAnalysis:
        """Analyze SQL string and extract components efficiently.

        Args:
            sql_string: The SQL string to analyze
            dialect: SQL dialect for parsing

        Returns:
            StatementAnalysis with extracted components
        """
        # Check cache first
        cache_key = sql_string.strip()
        if cache_key in self._analysis_cache:
            return self._analysis_cache[cache_key]

        # Use cache key for expression parsing performance
        parse_cache_key = (sql_string.strip(), str(dialect) if dialect else None)

        if parse_cache_key in self._parse_cache:
            expr = self._parse_cache[parse_cache_key]
        else:
            try:
                # Use maybe_parse for graceful fallback
                expr = exp.maybe_parse(sql_string, dialect=dialect)
                if expr is None:
                    # Fallback to parse_one for better error messages
                    import sqlglot

                    expr = sqlglot.parse_one(sql_string, read=dialect)

                # Cache the parsed expression
                if len(self._parse_cache) < self.cache_size:
                    self._parse_cache[parse_cache_key] = expr

            except (SQLGlotParseError, Exception) as e:
                logger.warning("Failed to parse SQL statement: %s", e)
                # Return minimal analysis for bad SQL
                return StatementAnalysis(
                    statement_type="Unknown",
                    expression=exp.Anonymous(this="UNKNOWN"),
                )

        return self.analyze_expression(expr)

    def analyze_expression(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
        validation_result: "Optional[ValidationResult]" = None,
    ) -> StatementAnalysis:
        """Analyze a SQLGlot expression directly, potentially using validation results for context."""
        # Check cache first (using expression.sql() as key)
        # This caching needs to be context-aware if analysis depends on prior steps (e.g. validation_result)
        # For simplicity, let's assume for now direct expression analysis is cacheable if validation_result is not used deeply.
        cache_key = expression.sql()  # Simplified cache key
        if cache_key in self._analysis_cache:
            # Potentially re-evaluate if critical context like validation_result changed, or make cache more sophisticated.
            # For now, return cached if expression is identical.
            return self._analysis_cache[cache_key]

        analysis = StatementAnalysis(
            statement_type=type(expression).__name__,
            expression=expression,
            table_name=self._extract_primary_table_name(expression),
            columns=self._extract_columns(expression),
            has_returning=bool(expression.find(exp.Returning)),
            is_from_select=self._is_insert_from_select(expression),
            parameters=self._extract_parameters(expression),  # This might use context.merged_parameters
            tables=self._extract_all_tables(expression),
            uses_subqueries=self._has_subqueries(expression),
            join_count=self._count_joins(expression),  # Simple count
            aggregate_functions=self._extract_aggregate_functions(expression),
        )

        # Enhanced complexity analysis, potentially using validation_result for context
        self._analyze_complexity(expression, analysis, validation_result)
        analysis.complexity_score = self._calculate_comprehensive_complexity_score(analysis)

        if len(self._analysis_cache) < self.cache_size:
            self._analysis_cache[cache_key] = analysis
        return analysis

    def _analyze_complexity(
        self,
        expression: exp.Expression,
        analysis: StatementAnalysis,
        validation_result: "Optional[ValidationResult]" = None,
    ) -> None:
        """Perform comprehensive complexity analysis, potentially using validation results."""
        # Analyze JOIN complexity
        join_analysis_res = self._analyze_joins(expression, validation_result)
        analysis.join_types = join_analysis_res["join_types"]
        analysis.potential_cartesian_products = join_analysis_res["potential_cartesian_products"]
        analysis.complexity_warnings.extend(join_analysis_res["warnings"])
        analysis.complexity_issues.extend(join_analysis_res["issues"])

        # Analyze subquery complexity
        subquery_analysis = self._analyze_subqueries(expression)
        analysis.max_subquery_depth = subquery_analysis["max_subquery_depth"]
        analysis.correlated_subquery_count = subquery_analysis["correlated_subquery_count"]
        analysis.complexity_warnings.extend(subquery_analysis["warnings"])
        analysis.complexity_issues.extend(subquery_analysis["issues"])

        # Analyze WHERE clause complexity
        where_analysis = self._analyze_where_clauses(expression)
        analysis.where_condition_count = where_analysis["total_where_conditions"]
        analysis.complexity_warnings.extend(where_analysis["warnings"])
        analysis.complexity_issues.extend(where_analysis["issues"])

        # Analyze function usage
        function_analysis = self._analyze_functions(expression)
        analysis.function_count = function_analysis["function_count"]
        analysis.complexity_warnings.extend(function_analysis["warnings"])
        analysis.complexity_issues.extend(function_analysis["issues"])

    def _analyze_joins(
        self, expression: exp.Expression, validation_result: "Optional[ValidationResult]" = None
    ) -> "dict[str, Any]":
        """Analyze JOIN operations. Can use validation_result to check for pre-identified cartesian products."""
        join_types: dict[str, int] = {}
        join_nodes = list(expression.find_all(exp.Join))
        join_count = len(join_nodes)
        warnings = []
        issues = []
        cartesian_products = 0
        for select in expression.find_all(exp.Select):
            if select.from_ and hasattr(select.from_, "expressions"):  # type: ignore[truthy-function]
                from_expressions = getattr(select.from_, "expressions", [])
                if len(from_expressions) > 1 and not list(select.args.get("joins", [])):
                    # Check if validation already flagged this specific type of implicit cartesian product
                    cartesian_products += 1
        if cartesian_products > 0:
            issues.append(
                f"Potential Cartesian product detected ({cartesian_products} instances from multiple FROM tables without JOIN)"
            )

        # Check explicit cross joins
        for join_node in join_nodes:
            if join_node.kind and join_node.kind.upper() == "CROSS":
                issues.append("Explicit CROSS JOIN found, potential Cartesian product.")
                cartesian_products += 1  # or a different counter for explicit cross joins
            elif not join_node.on and not join_node.using:  # type: ignore[truthy-function]
                issues.append(f"JOIN without ON/USING clause found ({join_node.sql()}), potential Cartesian product.")
                cartesian_products += 1

        if join_count > self.max_join_count:
            issues.append(f"Excessive number of joins ({join_count}), may cause performance issues")
        elif join_count > self.max_join_count // 2:
            warnings.append(f"High number of joins ({join_count}), monitor performance")

        return {
            "join_types": join_types,
            "potential_cartesian_products": cartesian_products,
            "warnings": warnings,
            "issues": issues,
            "join_count": join_count,  # Also return join_count for analysis.join_count
        }

    def _analyze_subqueries(self, expression: exp.Expression) -> "dict[str, Any]":
        """Analyze subquery complexity and nesting depth."""
        subqueries = list(expression.find_all(exp.Subquery))
        subquery_count = len(subqueries)
        max_depth = 0
        correlated_count = 0

        # Calculate maximum nesting depth
        def calculate_depth(expr: exp.Expression, current_depth: int = 0) -> int:
            max_found = current_depth
            for subquery in expr.find_all(exp.Subquery):
                if subquery.parent == expr:  # Direct child
                    depth = calculate_depth(subquery, current_depth + 1)
                    max_found = max(max_found, depth)
            return max_found

        max_depth = calculate_depth(expression)

        # Check for correlated subqueries (more expensive)
        for subquery in subqueries:
            # Simple heuristic: if subquery references outer columns
            # This is a simplified check - a full implementation would need more sophisticated analysis
            subquery_sql = subquery.sql().lower()
            if any(keyword in subquery_sql for keyword in ["exists", "not exists"]):
                correlated_count += 1

        warnings = []
        issues = []

        if max_depth > self.max_subquery_depth:
            issues.append(f"Excessive subquery nesting depth ({max_depth})")
        elif max_depth > self.max_subquery_depth // 2:
            warnings.append(f"High subquery nesting depth ({max_depth})")

        if subquery_count > HIGH_SUBQUERY_COUNT_THRESHOLD:
            warnings.append(f"High number of subqueries ({subquery_count})")

        if correlated_count > HIGH_CORRELATED_SUBQUERY_THRESHOLD:
            warnings.append(f"Multiple correlated subqueries detected ({correlated_count})")

        return {
            "max_subquery_depth": max_depth,
            "correlated_subquery_count": correlated_count,
            "warnings": warnings,
            "issues": issues,
        }

    def _analyze_where_clauses(self, expression: exp.Expression) -> "dict[str, Any]":
        """Analyze WHERE clause complexity."""
        where_clauses = list(expression.find_all(exp.Where))
        total_conditions = 0
        complex_conditions = 0

        for where_clause in where_clauses:
            # Count AND/OR conditions
            and_conditions = len(list(where_clause.find_all(exp.And)))
            or_conditions = len(list(where_clause.find_all(exp.Or)))
            total_conditions += and_conditions + or_conditions

            # Check for complex patterns
            if any(where_clause.find_all(pattern) for pattern in [exp.Like, exp.In, exp.Between]):
                complex_conditions += 1

        warnings = []
        issues = []

        if total_conditions > self.max_where_conditions:
            issues.append(f"Excessive WHERE conditions ({total_conditions})")
        elif total_conditions > self.max_where_conditions // 2:
            warnings.append(f"Complex WHERE clause ({total_conditions} conditions)")

        return {
            "total_where_conditions": total_conditions,
            "warnings": warnings,
            "issues": issues,
        }

    def _analyze_functions(self, expression: exp.Expression) -> "dict[str, Any]":
        """Analyze function usage and complexity."""
        function_types: dict[str, int] = {}
        function_count, nested_functions = 0, 0
        for func in expression.find_all(exp.Func):
            func_name = func.name.lower() if func.name else "unknown"
            function_types[func_name] = function_types.get(func_name, 0) + 1
            if list(func.find_all(exp.Func)):
                nested_functions += 1
            function_count += 1

        # Check for expensive functions
        expensive_functions = {"regexp", "regex", "like", "concat_ws", "group_concat"}
        expensive_count = sum(function_types.get(func, 0) for func in expensive_functions)

        warnings = []
        issues = []

        if function_count > self.max_function_calls:
            issues.append(f"Excessive function calls ({function_count})")
        elif function_count > self.max_function_calls // 2:
            warnings.append(f"High number of function calls ({function_count})")

        if expensive_count > EXPENSIVE_FUNCTION_THRESHOLD:
            warnings.append(f"Multiple expensive functions used ({expensive_count})")

        if nested_functions > NESTED_FUNCTION_THRESHOLD:
            warnings.append(f"Multiple nested function calls ({nested_functions})")

        return {
            "function_count": function_count,
            "warnings": warnings,
            "issues": issues,
        }

    @staticmethod
    def _calculate_comprehensive_complexity_score(analysis: StatementAnalysis) -> int:
        """Calculate an overall complexity score based on various metrics."""
        score = 0

        # Join complexity
        score += analysis.join_count * 3
        score += analysis.potential_cartesian_products * 20

        # Subquery complexity
        score += len([sq for sq in analysis.tables if "subquery" in sq.lower()]) * 5
        score += analysis.max_subquery_depth * 10
        score += analysis.correlated_subquery_count * 8

        # WHERE clause complexity
        score += analysis.where_condition_count * 2

        # Function complexity
        score += analysis.function_count * 1

        return score

    @staticmethod
    def _extract_primary_table_name(expr: exp.Expression) -> "Optional[str]":
        """Extract the primary table name from an expression."""
        if isinstance(expr, exp.Insert):
            if expr.this and hasattr(expr.this, "this"):
                # Handle schema.table cases
                table = expr.this
                if isinstance(table, exp.Table):
                    return table.name
                if hasattr(table, "name"):
                    return str(table.name)
        elif isinstance(expr, (exp.Update, exp.Delete)):
            if expr.this:
                return str(expr.this.name) if hasattr(expr.this, "name") else str(expr.this)
        elif isinstance(expr, exp.Select) and (from_clause := expr.find(exp.From)) and from_clause.this:
            return str(from_clause.this.name) if hasattr(from_clause.this, "name") else str(from_clause.this)
        return None

    @staticmethod
    def _extract_columns(expr: exp.Expression) -> "list[str]":
        """Extract column names from an expression."""
        columns: list[str] = []
        if isinstance(expr, exp.Insert):
            if expr.this and hasattr(expr.this, "expressions"):
                columns.extend(str(col_expr.name) for col_expr in expr.this.expressions if hasattr(col_expr, "name"))
        elif isinstance(expr, exp.Select):
            # Extract selected columns
            for projection in expr.expressions:
                if isinstance(projection, exp.Column):
                    columns.append(str(projection.name))
                elif hasattr(projection, "alias") and projection.alias:
                    columns.append(str(projection.alias))
                elif hasattr(projection, "name"):
                    columns.append(str(projection.name))

        return columns

    @staticmethod
    def _extract_all_tables(expr: exp.Expression) -> "list[str]":
        """Extract all table names referenced in the expression."""
        tables: list[str] = []
        for table in expr.find_all(exp.Table):
            if hasattr(table, "name"):
                table_name = str(table.name)
                if table_name not in tables:
                    tables.append(table_name)
        return tables

    @staticmethod
    def _is_insert_from_select(expr: exp.Expression) -> bool:
        """Check if this is an INSERT FROM SELECT pattern."""
        if not isinstance(expr, exp.Insert):
            return False
        return bool(expr.expression and isinstance(expr.expression, exp.Select))

    @staticmethod
    def _extract_parameters(_expr: exp.Expression) -> "dict[str, Any]":
        """Extract parameters from the expression."""
        # This could be enhanced to extract actual parameter placeholders
        return {}

    @staticmethod
    def _has_subqueries(expr: exp.Expression) -> bool:
        """Check if the expression contains subqueries."""
        return bool(expr.find(exp.Subquery))

    @staticmethod
    def _count_joins(expr: exp.Expression) -> int:
        """Count the number of joins in the expression."""
        return len(list(expr.find_all(exp.Join)))

    @staticmethod
    def _extract_aggregate_functions(expr: exp.Expression) -> "list[str]":
        """Extract aggregate function names from the expression."""
        aggregates: list[str] = []

        # Common aggregate function types in SQLGlot (using only those that exist)
        aggregate_types = [exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max]

        for agg_type in aggregate_types:
            if expr.find(agg_type):  # Check if this aggregate type exists in the expression
                func_name = agg_type.__name__.lower()
                if func_name not in aggregates:
                    aggregates.append(func_name)

        return aggregates

    def clear_cache(self) -> None:
        """Clear both parse and analysis caches."""
        self._parse_cache.clear()
        self._analysis_cache.clear()
