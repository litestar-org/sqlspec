"""SQL statement analyzer for extracting metadata and complexity metrics."""

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp, parse_one
from sqlglot.errors import ParseError as SQLGlotParseError

from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.result_types import AnalysisFinding
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.pipelines.context import SQLProcessingContext
    from sqlspec.statement.sql import SQLConfig

__all__ = ("StatementAnalysis", "StatementAnalyzer")

# Constants for statement analysis
HIGH_SUBQUERY_COUNT_THRESHOLD = 10
"""Threshold for flagging high number of subqueries."""

HIGH_CORRELATED_SUBQUERY_THRESHOLD = 3
"""Threshold for flagging multiple correlated subqueries."""

EXPENSIVE_FUNCTION_THRESHOLD = 5
"""Threshold for flagging multiple expensive functions."""

NESTED_FUNCTION_THRESHOLD = 3
"""Threshold for flagging multiple nested function calls."""

logger = get_logger("pipelines.analyzers")


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

    # Additional attributes for aggregator compatibility
    subquery_count: int = 0
    """Total number of subqueries"""
    operations: "list[str]" = field(default_factory=list)
    """SQL operations performed (SELECT, JOIN, etc.)"""
    has_aggregation: bool = False
    """Whether query uses aggregation functions"""
    has_window_functions: bool = False
    """Whether query uses window functions"""
    cte_count: int = 0
    """Number of CTEs (Common Table Expressions)"""


class StatementAnalyzer(ProcessorProtocol):
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

    def process(
        self, expression: "Optional[exp.Expression]", context: "SQLProcessingContext"
    ) -> "Optional[exp.Expression]":
        """Process the SQL expression to extract analysis metadata and store it in the context."""
        if expression is None:
            return None

        CorrelationContext.get()
        start_time = time.perf_counter()

        if not context.config.enable_analysis:
            return expression

        analysis_result_obj = self.analyze_expression(expression, context.dialect, context.config)

        duration = time.perf_counter() - start_time

        # Add analysis findings to context
        if analysis_result_obj.complexity_warnings:
            for warning in analysis_result_obj.complexity_warnings:
                finding = AnalysisFinding(key="complexity_warning", value=warning, processor=self.__class__.__name__)
                context.analysis_findings.append(finding)

        if analysis_result_obj.complexity_issues:
            for issue in analysis_result_obj.complexity_issues:
                finding = AnalysisFinding(key="complexity_issue", value=issue, processor=self.__class__.__name__)
                context.analysis_findings.append(finding)

        # Store metadata in context
        context.metadata[self.__class__.__name__] = {
            "duration_ms": duration * 1000,
            "statement_type": analysis_result_obj.statement_type,
            "table_count": len(analysis_result_obj.tables),
            "has_subqueries": analysis_result_obj.uses_subqueries,
            "join_count": analysis_result_obj.join_count,
            "complexity_score": analysis_result_obj.complexity_score,
        }
        return expression

    def analyze_statement(self, sql_string: str, dialect: "DialectType" = None) -> StatementAnalysis:
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
                expr = exp.maybe_parse(sql_string, dialect=dialect)
                if expr is None:
                    expr = parse_one(sql_string, dialect=dialect)

                # Check if the parsed expression is a valid SQL statement type
                # Simple expressions like Alias or Identifier are not valid SQL statements
                valid_statement_types = (
                    exp.Select,
                    exp.Insert,
                    exp.Update,
                    exp.Delete,
                    exp.Create,
                    exp.Drop,
                    exp.Alter,
                    exp.Merge,
                    exp.Command,
                    exp.Set,
                    exp.Show,
                    exp.Describe,
                    exp.Use,
                    exp.Union,
                    exp.Intersect,
                    exp.Except,
                )
                if not isinstance(expr, valid_statement_types):
                    logger.warning("Parsed expression is not a valid SQL statement: %s", type(expr).__name__)
                    return StatementAnalysis(statement_type="Unknown", expression=exp.Anonymous(this="UNKNOWN"))

                if len(self._parse_cache) < self.cache_size:
                    self._parse_cache[parse_cache_key] = expr
            except (SQLGlotParseError, Exception) as e:
                logger.warning("Failed to parse SQL statement: %s", e)
                return StatementAnalysis(statement_type="Unknown", expression=exp.Anonymous(this="UNKNOWN"))

        return self.analyze_expression(expr)

    def analyze_expression(
        self, expression: exp.Expression, dialect: "DialectType" = None, config: "Optional[SQLConfig]" = None
    ) -> StatementAnalysis:
        """Analyze a SQLGlot expression directly, potentially using validation results for context."""
        # Check cache first (using expression.sql() as key)
        # This caching needs to be context-aware if analysis depends on prior steps (e.g. validation_result)
        # For simplicity, let's assume for now direct expression analysis is cacheable if validation_result is not used deeply.
        cache_key = expression.sql()  # Simplified cache key
        if cache_key in self._analysis_cache:
            return self._analysis_cache[cache_key]

        analysis = StatementAnalysis(
            statement_type=type(expression).__name__,
            expression=expression,
            table_name=self._extract_primary_table_name(expression),
            columns=self._extract_columns(expression),
            has_returning=bool(expression.find(exp.Returning)),
            is_from_select=self._is_insert_from_select(expression),
            parameters=self._extract_parameters(expression),
            tables=self._extract_all_tables(expression),
            uses_subqueries=self._has_subqueries(expression),
            join_count=self._count_joins(expression),
            aggregate_functions=self._extract_aggregate_functions(expression),
        )
        # Calculate subquery_count and cte_count before complexity analysis
        analysis.subquery_count = len(list(expression.find_all(exp.Subquery)))
        # Also need to account for IN/EXISTS subqueries that aren't wrapped in Subquery nodes
        for in_clause in expression.find_all(exp.In):
            if in_clause.args.get("query") and isinstance(in_clause.args.get("query"), exp.Select):
                analysis.subquery_count += 1
        for exists_clause in expression.find_all(exp.Exists):
            if exists_clause.this and isinstance(exists_clause.this, exp.Select):
                analysis.subquery_count += 1

        # Calculate CTE count before complexity score
        analysis.cte_count = len(list(expression.find_all(exp.CTE)))

        self._analyze_complexity(expression, analysis)
        analysis.complexity_score = self._calculate_comprehensive_complexity_score(analysis)
        analysis.operations = self._extract_operations(expression)
        analysis.has_aggregation = len(analysis.aggregate_functions) > 0
        analysis.has_window_functions = self._has_window_functions(expression)

        if len(self._analysis_cache) < self.cache_size:
            self._analysis_cache[cache_key] = analysis
        return analysis

    def _analyze_complexity(self, expression: exp.Expression, analysis: StatementAnalysis) -> None:
        """Perform comprehensive complexity analysis."""
        self._analyze_joins(expression, analysis)
        self._analyze_subqueries(expression, analysis)
        self._analyze_where_clauses(expression, analysis)
        self._analyze_functions(expression, analysis)

    def _analyze_joins(self, expression: exp.Expression, analysis: StatementAnalysis) -> None:
        """Analyze JOIN operations for potential issues."""
        join_nodes = list(expression.find_all(exp.Join))
        analysis.join_count = len(join_nodes)

        warnings = []
        issues = []
        cartesian_products = 0

        for select in expression.find_all(exp.Select):
            from_clause = select.args.get("from")
            if from_clause and hasattr(from_clause, "expressions") and len(from_clause.expressions) > 1:
                # This logic checks for multiple tables in FROM without explicit JOINs
                # It's a simplified check for potential cartesian products
                cartesian_products += 1

        if cartesian_products > 0:
            issues.append(
                f"Potential Cartesian product detected ({cartesian_products} instances from multiple FROM tables without JOIN)"
            )

        for join_node in join_nodes:
            join_type = join_node.kind.upper() if join_node.kind else "INNER"
            analysis.join_types[join_type] = analysis.join_types.get(join_type, 0) + 1

            if join_type == "CROSS":
                issues.append("Explicit CROSS JOIN found, potential Cartesian product.")
                cartesian_products += 1
            elif not join_node.args.get("on") and not join_node.args.get("using") and join_type != "NATURAL":
                issues.append(f"JOIN ({join_node.sql()}) without ON/USING clause, potential Cartesian product.")
                cartesian_products += 1

        if analysis.join_count > self.max_join_count:
            issues.append(f"Excessive number of joins ({analysis.join_count}), may cause performance issues")
        elif analysis.join_count > self.max_join_count // 2:
            warnings.append(f"High number of joins ({analysis.join_count}), monitor performance")

        analysis.potential_cartesian_products = cartesian_products
        analysis.complexity_warnings.extend(warnings)
        analysis.complexity_issues.extend(issues)

    def _analyze_subqueries(self, expression: exp.Expression, analysis: StatementAnalysis) -> None:
        """Analyze subquery complexity and nesting depth."""
        subqueries: list[exp.Expression] = list(expression.find_all(exp.Subquery))
        subqueries.extend(
            query
            for in_clause in expression.find_all(exp.In)
            if (query := in_clause.args.get("query")) and isinstance(query, exp.Select)
        )
        subqueries.extend(
            [
                exists_clause.this
                for exists_clause in expression.find_all(exp.Exists)
                if exists_clause.this and isinstance(exists_clause.this, exp.Select)
            ]
        )

        analysis.subquery_count = len(subqueries)
        max_depth = 0
        correlated_count = 0

        # Calculate maximum nesting depth - simpler approach
        def calculate_depth(expr: exp.Expression) -> int:
            """Calculate the maximum depth of nested SELECT statements."""
            max_depth = 0

            # Find all SELECT statements
            select_statements = list(expr.find_all(exp.Select))

            for select in select_statements:
                # Count how many parent SELECTs this one has
                depth = 0
                current = select.parent
                while current:
                    # Check if parent is a SELECT or if it's inside a SELECT via Subquery/IN/EXISTS
                    if isinstance(current, exp.Select):
                        depth += 1
                    elif isinstance(current, (exp.Subquery, exp.In, exp.Exists)):
                        # These nodes can contain SELECTs, check their parent
                        parent = current.parent
                        while parent and not isinstance(parent, exp.Select):
                            parent = parent.parent
                        if parent:
                            current = parent
                            continue
                    current = current.parent if current else None

                max_depth = max(max_depth, depth)

            return max_depth

        max_depth = calculate_depth(expression)
        outer_tables = {tbl.alias or tbl.name for tbl in expression.find_all(exp.Table)}
        for subquery in subqueries:
            for col in subquery.find_all(exp.Column):
                if col.table and col.table in outer_tables:
                    correlated_count += 1
                    break

        warnings = []
        issues = []

        if max_depth > self.max_subquery_depth:
            issues.append(f"Excessive subquery nesting depth ({max_depth})")
        elif max_depth > self.max_subquery_depth // 2:
            warnings.append(f"High subquery nesting depth ({max_depth})")

        if analysis.subquery_count > HIGH_SUBQUERY_COUNT_THRESHOLD:
            warnings.append(f"High number of subqueries ({analysis.subquery_count})")

        if correlated_count > HIGH_CORRELATED_SUBQUERY_THRESHOLD:
            warnings.append(f"Multiple correlated subqueries detected ({correlated_count})")

        analysis.max_subquery_depth = max_depth
        analysis.correlated_subquery_count = correlated_count
        analysis.complexity_warnings.extend(warnings)
        analysis.complexity_issues.extend(issues)

    def _analyze_where_clauses(self, expression: exp.Expression, analysis: StatementAnalysis) -> None:
        """Analyze WHERE clause complexity."""
        where_clauses = list(expression.find_all(exp.Where))
        total_conditions = 0

        for where_clause in where_clauses:
            total_conditions += len(list(where_clause.find_all(exp.And)))
            total_conditions += len(list(where_clause.find_all(exp.Or)))

        warnings = []
        issues = []

        if total_conditions > self.max_where_conditions:
            issues.append(f"Excessive WHERE conditions ({total_conditions})")
        elif total_conditions > self.max_where_conditions // 2:
            warnings.append(f"Complex WHERE clause ({total_conditions} conditions)")

        analysis.where_condition_count = total_conditions
        analysis.complexity_warnings.extend(warnings)
        analysis.complexity_issues.extend(issues)

    def _analyze_functions(self, expression: exp.Expression, analysis: StatementAnalysis) -> None:
        """Analyze function usage and complexity."""
        function_types: dict[str, int] = {}
        nested_functions = 0
        function_count = 0
        for func in expression.find_all(exp.Func):
            func_name = func.name.lower() if func.name else "unknown"
            function_types[func_name] = function_types.get(func_name, 0) + 1
            if any(isinstance(arg, exp.Func) for arg in func.args.values()):
                nested_functions += 1
            function_count += 1

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

        analysis.function_count = function_count
        analysis.complexity_warnings.extend(warnings)
        analysis.complexity_issues.extend(issues)

    @staticmethod
    def _calculate_comprehensive_complexity_score(analysis: StatementAnalysis) -> int:
        """Calculate an overall complexity score based on various metrics."""
        score = 0

        # Join complexity
        score += analysis.join_count * 3
        score += analysis.potential_cartesian_products * 20

        # Subquery complexity
        score += analysis.subquery_count * 5  # Use actual subquery count
        score += analysis.max_subquery_depth * 10
        score += analysis.correlated_subquery_count * 8

        # CTE complexity (CTEs are complex, especially recursive ones)
        score += analysis.cte_count * 7

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
        # For now, _expr is unused but will be used in future enhancements
        _ = _expr
        return {}

    @staticmethod
    def _has_subqueries(expr: exp.Expression) -> bool:
        """Check if the expression contains subqueries.

        Note: Due to sqlglot parser inconsistency, subqueries in IN clauses
        are not wrapped in Subquery nodes, so we need additional detection.
        CTEs are not considered subqueries.
        """
        # Standard subquery detection
        if expr.find(exp.Subquery):
            return True

        # sqlglot compatibility: IN clauses with SELECT need explicit handling
        for in_clause in expr.find_all(exp.In):
            query_node = in_clause.args.get("query")
            if query_node and isinstance(query_node, exp.Select):
                return True

        # sqlglot compatibility: EXISTS clauses with SELECT need explicit handling
        for exists_clause in expr.find_all(exp.Exists):
            if exists_clause.this and isinstance(exists_clause.this, exp.Select):
                return True

        # Check for multiple SELECT statements (indicates subqueries)
        # but exclude those within CTEs
        select_statements = []
        for select in expr.find_all(exp.Select):
            # Check if this SELECT is inside a CTE
            parent = select.parent
            is_in_cte = False
            while parent:
                if isinstance(parent, exp.CTE):
                    is_in_cte = True
                    break
                parent = parent.parent
            if not is_in_cte:
                select_statements.append(select)

        return len(select_statements) > 1

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

    @staticmethod
    def _extract_operations(expr: exp.Expression) -> "list[str]":
        """Extract SQL operations performed."""
        operations = []

        # Main operation
        if isinstance(expr, exp.Select):
            operations.append("SELECT")
        elif isinstance(expr, exp.Insert):
            operations.append("INSERT")
        elif isinstance(expr, exp.Update):
            operations.append("UPDATE")
        elif isinstance(expr, exp.Delete):
            operations.append("DELETE")
        elif isinstance(expr, exp.Create):
            operations.append("CREATE")
        elif isinstance(expr, exp.Drop):
            operations.append("DROP")
        elif isinstance(expr, exp.Alter):
            operations.append("ALTER")
        if expr.find(exp.Join):
            operations.append("JOIN")
        if expr.find(exp.Group):
            operations.append("GROUP BY")
        if expr.find(exp.Order):
            operations.append("ORDER BY")
        if expr.find(exp.Having):
            operations.append("HAVING")
        if expr.find(exp.Union):
            operations.append("UNION")
        if expr.find(exp.Intersect):
            operations.append("INTERSECT")
        if expr.find(exp.Except):
            operations.append("EXCEPT")

        return operations

    @staticmethod
    def _has_window_functions(expr: exp.Expression) -> bool:
        """Check if expression uses window functions."""
        return bool(expr.find(exp.Window))
