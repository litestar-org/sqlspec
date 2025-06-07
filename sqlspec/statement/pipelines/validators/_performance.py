"""Performance validator for SQL query optimization."""

from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

import sqlglot.expressions as exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.validators.base import BaseValidator, ProcessorResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("JoinCondition", "PerformanceAnalysis", "PerformanceConfig", "PerformanceIssue", "PerformanceValidator")

# Constants
DEEP_NESTING_THRESHOLD = 2


@dataclass
class PerformanceConfig:
    """Configuration for performance validation."""

    max_joins: int = 5
    max_subqueries: int = 3
    max_union_branches: int = 5
    warn_on_cartesian: bool = True
    warn_on_missing_index: bool = True
    complexity_threshold: int = 50
    analyze_execution_plan: bool = False


@dataclass
class PerformanceIssue:
    """Represents a performance issue found during validation."""

    issue_type: str  # "cartesian", "excessive_joins", "missing_index", etc.
    severity: str  # "warning", "error", "critical"
    description: str
    impact: str  # Expected performance impact
    recommendation: str
    location: "Optional[str]" = None  # SQL fragment


@dataclass
class JoinCondition:
    """Information about a join condition."""

    left_table: str
    right_table: str
    condition: "Optional[exp.Expression]"
    join_type: str


@dataclass
class PerformanceAnalysis:
    """Tracks performance metrics during AST traversal."""

    # Join analysis
    join_count: int = 0
    join_types: "dict[str, int]" = field(default_factory=dict)
    join_conditions: "list[JoinCondition]" = field(default_factory=list)
    tables: "set[str]" = field(default_factory=set)

    # Subquery analysis
    subquery_count: int = 0
    max_subquery_depth: int = 0
    current_subquery_depth: int = 0
    correlated_subqueries: int = 0

    # Complexity metrics
    where_conditions: int = 0
    group_by_columns: int = 0
    order_by_columns: int = 0
    distinct_operations: int = 0
    union_branches: int = 0

    # Anti-patterns
    select_star_count: int = 0
    implicit_conversions: int = 0
    non_sargable_predicates: int = 0


class PerformanceValidator(BaseValidator):
    """Comprehensive query performance validator.

    Validates query performance by detecting:
    - Cartesian products
    - Excessive joins
    - Deep subquery nesting
    - Performance anti-patterns
    - High query complexity
    """

    def __init__(self, config: "Optional[PerformanceConfig]" = None) -> None:
        """Initialize the performance validator.

        Args:
            config: Configuration for performance validation
        """
        super().__init__()
        self.config = config or PerformanceConfig()

    def _process_internal(self, context: "SQLProcessingContext") -> ProcessorResult:
        """Process SQL statement for performance validation.

        Args:
            context: The SQL processing context

        Returns:
            ProcessorResult with performance analysis
        """
        if context.current_expression is None:
            return self._create_result(
                context.current_expression,
                is_safe=True,
                risk_level=RiskLevel.SKIP,
                issues=["No expression to analyze"],
                metadata={"skipped": True},
            )

        # Performance analysis state
        analysis = PerformanceAnalysis()
        issues: list[PerformanceIssue] = []

        # Single traversal for all checks
        self._analyze_expression(context.current_expression, analysis)

        # Check for cartesian products
        if self.config.warn_on_cartesian:
            cartesian_issues = self._check_cartesian_products(analysis)
            issues.extend(cartesian_issues)

        # Check join complexity
        if analysis.join_count > self.config.max_joins:
            issues.append(
                PerformanceIssue(
                    issue_type="excessive_joins",
                    severity="warning",
                    description=f"Query has {analysis.join_count} joins (max: {self.config.max_joins})",
                    impact="Exponential performance degradation with data growth",
                    recommendation="Consider breaking into smaller queries or denormalizing",
                )
            )

        # Check subquery depth
        if analysis.max_subquery_depth > self.config.max_subqueries:
            issues.append(
                PerformanceIssue(
                    issue_type="deep_nesting",
                    severity="warning",
                    description=f"Query has {analysis.max_subquery_depth} levels of subqueries",
                    impact="Poor query optimizer performance, difficult to cache",
                    recommendation="Flatten subqueries using CTEs or temporary tables",
                )
            )

        # Check for performance anti-patterns
        pattern_issues = self._check_antipatterns(analysis)
        issues.extend(pattern_issues)

        # Calculate overall complexity score
        complexity_score = self._calculate_complexity(analysis)

        # Determine risk level
        risk_level = self._determine_risk_level(issues, complexity_score)

        # Build metadata
        metadata = {
            "performance_issues": [self._issue_to_dict(issue) for issue in issues],
            "complexity_score": complexity_score,
            "join_analysis": {
                "total_joins": analysis.join_count,
                "join_types": dict(analysis.join_types),
                "tables_involved": list(analysis.tables),
            },
            "subquery_analysis": {
                "max_depth": analysis.max_subquery_depth,
                "total_subqueries": analysis.subquery_count,
                "correlated_subqueries": analysis.correlated_subqueries,
            },
            "recommendations": [issue.recommendation for issue in issues],
        }

        # Return result
        return self._create_result(
            context.current_expression,
            is_safe=len(issues) == 0 or risk_level == RiskLevel.LOW,
            risk_level=risk_level,
            issues=[issue.description for issue in issues],
            warnings=[issue.description for issue in issues if issue.severity == "warning"],
            metadata=metadata,
        )

    def _analyze_expression(self, expr: "exp.Expression", analysis: PerformanceAnalysis, depth: int = 0) -> None:
        """Single-pass traversal to collect all performance metrics.

        Args:
            expr: Expression to analyze
            analysis: Analysis state to update
            depth: Current recursion depth
        """
        # Track subquery depth
        if isinstance(expr, exp.Subquery):
            analysis.subquery_count += 1
            analysis.current_subquery_depth = max(analysis.current_subquery_depth, depth + 1)
            analysis.max_subquery_depth = max(analysis.max_subquery_depth, analysis.current_subquery_depth)

            # Check if correlated
            if self._is_correlated_subquery(expr):
                analysis.correlated_subqueries += 1

        # Analyze joins
        elif isinstance(expr, exp.Join):
            analysis.join_count += 1
            join_type = expr.args.get("kind", "INNER").upper()
            analysis.join_types[join_type] = analysis.join_types.get(join_type, 0) + 1

            # Extract join condition
            condition = expr.args.get("on")
            left_table = self._get_table_name(expr.parent) if expr.parent else "unknown"
            right_table = self._get_table_name(expr.this)

            analysis.join_conditions.append(
                JoinCondition(left_table=left_table, right_table=right_table, condition=condition, join_type=join_type)
            )

            analysis.tables.add(left_table)
            analysis.tables.add(right_table)

        # Track other complexity factors
        elif isinstance(expr, exp.Where):
            analysis.where_conditions += len(list(expr.find_all(exp.Predicate)))

        elif isinstance(expr, exp.Group):
            analysis.group_by_columns += len(expr.expressions) if hasattr(expr, "expressions") else 0

        elif isinstance(expr, exp.Order):
            analysis.order_by_columns += len(expr.expressions) if hasattr(expr, "expressions") else 0

        elif isinstance(expr, exp.Distinct):
            analysis.distinct_operations += 1

        elif isinstance(expr, exp.Union):
            analysis.union_branches += 1

        elif isinstance(expr, exp.Star):
            analysis.select_star_count += 1

        # Check for non-sargable predicates
        if isinstance(expr, exp.Predicate) and self._is_non_sargable(expr):
            analysis.non_sargable_predicates += 1

        # Recursive traversal
        for child in expr.args.values():
            if isinstance(child, exp.Expression):
                self._analyze_expression(child, analysis, depth)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, exp.Expression):
                        self._analyze_expression(item, analysis, depth)

    def _check_cartesian_products(self, analysis: PerformanceAnalysis) -> "list[PerformanceIssue]":
        """Detect potential cartesian products from join analysis.

        Args:
            analysis: Performance analysis state

        Returns:
            List of cartesian product issues
        """
        issues = []

        # Group joins by table pairs
        join_graph: dict[str, set[str]] = defaultdict(set)
        for condition in analysis.join_conditions:
            if condition.condition is None:  # CROSS JOIN
                issues.append(
                    PerformanceIssue(
                        issue_type="cartesian_product",
                        severity="critical",
                        description=f"Explicit CROSS JOIN between {condition.left_table} and {condition.right_table}",
                        impact="Result set grows exponentially (MxN rows)",
                        recommendation="Add join condition or use WHERE clause",
                    )
                )
            else:
                # Build join graph
                join_graph[condition.left_table].add(condition.right_table)
                join_graph[condition.right_table].add(condition.left_table)

        # Check for disconnected tables (implicit cartesian)
        if len(analysis.tables) > 1:
            connected = self._find_connected_components(join_graph, analysis.tables)
            if len(connected) > 1:
                disconnected_tables = [list(component) for component in connected if len(component) > 0]
                issues.append(
                    PerformanceIssue(
                        issue_type="implicit_cartesian",
                        severity="critical",
                        description=f"Tables form disconnected groups: {disconnected_tables}",
                        impact="Implicit cartesian product between table groups",
                        recommendation="Add join conditions between table groups",
                    )
                )

        return issues

    def _check_antipatterns(self, analysis: PerformanceAnalysis) -> "list[PerformanceIssue]":
        """Check for common performance anti-patterns.

        Args:
            analysis: Performance analysis state

        Returns:
            List of anti-pattern issues
        """
        issues = []

        # SELECT * in production queries
        if analysis.select_star_count > 0:
            issues.append(
                PerformanceIssue(
                    issue_type="select_star",
                    severity="warning",
                    description=f"Query uses SELECT * ({analysis.select_star_count} occurrences)",
                    impact="Fetches unnecessary columns, breaks with schema changes",
                    recommendation="Explicitly list required columns",
                )
            )

        # Non-sargable predicates
        if analysis.non_sargable_predicates > 0:
            issues.append(
                PerformanceIssue(
                    issue_type="non_sargable",
                    severity="warning",
                    description=f"Query has {analysis.non_sargable_predicates} non-sargable predicates",
                    impact="Cannot use indexes effectively",
                    recommendation="Rewrite predicates to be sargable (avoid functions on columns)",
                )
            )

        # Correlated subqueries
        if analysis.correlated_subqueries > 0:
            issues.append(
                PerformanceIssue(
                    issue_type="correlated_subquery",
                    severity="warning",
                    description=f"Query has {analysis.correlated_subqueries} correlated subqueries",
                    impact="Subquery executes once per outer row (N+1 problem)",
                    recommendation="Rewrite using JOIN or window functions",
                )
            )

        # Deep nesting
        if analysis.max_subquery_depth > DEEP_NESTING_THRESHOLD:
            issues.append(
                PerformanceIssue(
                    issue_type="deep_nesting",
                    severity="warning",
                    description=f"Query has {analysis.max_subquery_depth} levels of nesting",
                    impact="Difficult for optimizer, hard to maintain",
                    recommendation="Use CTEs to flatten query structure",
                )
            )

        return issues

    def _calculate_complexity(self, analysis: PerformanceAnalysis) -> int:
        """Calculate overall query complexity score.

        Args:
            analysis: Performance analysis state

        Returns:
            Complexity score
        """
        score = 0

        # Join complexity (exponential factor)
        score += analysis.join_count**2 * 5

        # Subquery complexity
        score += analysis.subquery_count * 10
        score += analysis.correlated_subqueries * 20
        score += analysis.max_subquery_depth * 15

        # Predicate complexity
        score += analysis.where_conditions * 2

        # Grouping/sorting complexity
        score += analysis.group_by_columns * 3
        score += analysis.order_by_columns * 2
        score += analysis.distinct_operations * 5

        # Anti-pattern penalties
        score += analysis.select_star_count * 5
        score += analysis.non_sargable_predicates * 10

        # Union complexity
        score += analysis.union_branches * 8

        return score

    def _determine_risk_level(self, issues: "list[PerformanceIssue]", complexity_score: int) -> RiskLevel:
        """Determine overall risk level from issues and complexity.

        Args:
            issues: List of performance issues
            complexity_score: Calculated complexity score

        Returns:
            Overall risk level
        """
        if any(issue.severity == "critical" for issue in issues):
            return RiskLevel.CRITICAL

        if complexity_score > self.config.complexity_threshold * 2:
            return RiskLevel.HIGH

        if any(issue.severity == "error" for issue in issues):
            return RiskLevel.HIGH

        if complexity_score > self.config.complexity_threshold:
            return RiskLevel.MEDIUM

        if any(issue.severity == "warning" for issue in issues):
            return RiskLevel.LOW

        return RiskLevel.SKIP

    def _is_correlated_subquery(self, subquery: "exp.Subquery") -> bool:
        """Check if subquery is correlated (references outer query).

        Args:
            subquery: Subquery expression

        Returns:
            True if correlated
        """
        # Simplified check - look for column references without table qualifiers
        # In a real implementation, would need to track scope
        return any(not col.table for col in subquery.find_all(exp.Column))

    def _is_non_sargable(self, predicate: "exp.Predicate") -> bool:
        """Check if predicate is non-sargable (can't use index).

        Args:
            predicate: Predicate expression

        Returns:
            True if non-sargable
        """
        # Check for functions on columns
        if hasattr(predicate, "left") and isinstance(predicate.left, exp.Func):
            # Function on left side of predicate
            for _col in predicate.left.find_all(exp.Column):
                return True  # Function wrapping column

        # Check for type conversions
        return bool(hasattr(predicate, "left") and isinstance(predicate.left, exp.Cast))

    def _get_table_name(self, expr: "Optional[exp.Expression]") -> str:
        """Extract table name from expression.

        Args:
            expr: Expression to extract from

        Returns:
            Table name or "unknown"
        """
        if expr is None:
            return "unknown"

        if isinstance(expr, exp.Table):
            return expr.name

        # Try to find table in expression
        tables = list(expr.find_all(exp.Table))
        if tables:
            return tables[0].name

        return "unknown"

    def _find_connected_components(self, graph: "dict[str, set[str]]", nodes: "set[str]") -> "list[set[str]]":
        """Find connected components in join graph.

        Args:
            graph: Adjacency list representation
            nodes: All nodes to consider

        Returns:
            List of connected components
        """
        visited = set()
        components = []

        def dfs(node: str, component: "set[str]") -> None:
            """Depth-first search to find component."""
            visited.add(node)
            component.add(node)
            for neighbor in graph.get(node, set()):
                if neighbor not in visited and neighbor in nodes:
                    dfs(neighbor, component)

        for node in nodes:
            if node not in visited:
                component: set[str] = set()
                dfs(node, component)
                components.append(component)

        return components

    def _issue_to_dict(self, issue: PerformanceIssue) -> "dict[str, Any]":
        """Convert PerformanceIssue to dictionary.

        Args:
            issue: The performance issue

        Returns:
            Dictionary representation
        """
        return {
            "issue_type": issue.issue_type,
            "severity": issue.severity,
            "description": issue.description,
            "impact": issue.impact,
            "recommendation": issue.recommendation,
            "location": issue.location,
        }
