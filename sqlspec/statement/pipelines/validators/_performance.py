"""Performance validator for SQL query optimization."""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import expressions as exp
from sqlglot.optimizer import (
    eliminate_joins,
    eliminate_subqueries,
    merge_subqueries,
    normalize_identifiers,
    optimize_joins,
    pushdown_predicates,
    pushdown_projections,
    simplify,
)

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.validators.base import BaseValidator

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = (
    "JoinCondition",
    "OptimizationOpportunity",
    "PerformanceAnalysis",
    "PerformanceConfig",
    "PerformanceIssue",
    "PerformanceValidator",
)

logger = logging.getLogger(__name__)

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

    # SQLGlot optimization analysis
    enable_optimization_analysis: bool = True
    suggest_optimizations: bool = True
    optimization_threshold: float = 0.2  # 20% potential improvement to flag
    max_optimization_attempts: int = 3


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
class OptimizationOpportunity:
    """Represents a potential optimization for the query."""

    optimization_type: str  # "join_elimination", "predicate_pushdown", etc.
    description: str
    potential_improvement: float  # Estimated improvement factor (0.0 to 1.0)
    complexity_reduction: int  # Estimated complexity score reduction
    recommendation: str
    optimized_sql: "Optional[str]" = None


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

    # SQLGlot optimization analysis
    optimization_opportunities: "list[OptimizationOpportunity]" = field(default_factory=list)
    original_complexity: int = 0
    optimized_complexity: int = 0
    potential_improvement: float = 0.0


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

    def validate(self, expression: "exp.Expression", context: "SQLProcessingContext") -> None:
        """Validate SQL statement for performance issues.

        Args:
            expression: The SQL expression to validate
            context: The SQL processing context
        """

        # Performance analysis state
        analysis = PerformanceAnalysis()

        # Single traversal for all checks
        self._analyze_expression(expression, analysis)

        # Calculate baseline complexity
        analysis.original_complexity = self._calculate_complexity(analysis)

        # Perform SQLGlot optimization analysis if enabled
        if self.config.enable_optimization_analysis:
            self._analyze_optimization_opportunities(expression, analysis, context)

        # Check for cartesian products
        if self.config.warn_on_cartesian:
            cartesian_issues = self._check_cartesian_products(analysis)
            for issue in cartesian_issues:
                self.add_error(
                    context,
                    message=issue.description,
                    code=issue.issue_type,
                    risk_level=self._severity_to_risk_level(issue.severity),
                    expression=expression,
                )

        # Check join complexity
        if analysis.join_count > self.config.max_joins:
            self.add_error(
                context,
                message=f"Query has {analysis.join_count} joins (max: {self.config.max_joins})",
                code="excessive-joins",
                risk_level=RiskLevel.MEDIUM,
                expression=expression,
            )

        # Check subquery depth
        if analysis.max_subquery_depth > self.config.max_subqueries:
            self.add_error(
                context,
                message=f"Query has {analysis.max_subquery_depth} levels of subqueries",
                code="deep-nesting",
                risk_level=RiskLevel.MEDIUM,
                expression=expression,
            )

        # Check for performance anti-patterns
        pattern_issues = self._check_antipatterns(analysis)
        for issue in pattern_issues:
            self.add_error(
                context,
                message=issue.description,
                code=issue.issue_type,
                risk_level=self._severity_to_risk_level(issue.severity),
                expression=expression,
            )

        # Calculate overall complexity score
        complexity_score = self._calculate_complexity(analysis)

        # Build metadata
        context.metadata[self.__class__.__name__] = {
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
            "optimization_analysis": {
                "opportunities": [self._optimization_to_dict(opt) for opt in analysis.optimization_opportunities],
                "original_complexity": analysis.original_complexity,
                "optimized_complexity": analysis.optimized_complexity,
                "potential_improvement": analysis.potential_improvement,
                "optimization_enabled": self.config.enable_optimization_analysis,
            },
        }

    @staticmethod
    def _severity_to_risk_level(severity: str) -> RiskLevel:
        """Convert severity string to RiskLevel."""
        mapping = {
            "critical": RiskLevel.CRITICAL,
            "error": RiskLevel.HIGH,
            "warning": RiskLevel.MEDIUM,
            "info": RiskLevel.LOW,
        }
        return mapping.get(severity.lower(), RiskLevel.MEDIUM)

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

    @staticmethod
    def _check_antipatterns(analysis: PerformanceAnalysis) -> "list[PerformanceIssue]":
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
                    severity="info",  # Changed to info level
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

    @staticmethod
    def _calculate_complexity(analysis: PerformanceAnalysis) -> int:
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

    @staticmethod
    def _is_correlated_subquery(subquery: "exp.Subquery") -> bool:
        """Check if subquery is correlated (references outer query).

        Args:
            subquery: Subquery expression

        Returns:
            True if correlated
        """
        # Simplified check - look for column references without table qualifiers
        # In a real implementation, would need to track scope
        return any(not col.table for col in subquery.find_all(exp.Column))

    @staticmethod
    def _get_table_name(expr: "Optional[exp.Expression]") -> str:
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

    @staticmethod
    def _find_connected_components(graph: "dict[str, set[str]]", nodes: "set[str]") -> "list[set[str]]":
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

    def _analyze_optimization_opportunities(
        self, expression: "exp.Expression", analysis: PerformanceAnalysis, context: "SQLProcessingContext"
    ) -> None:
        """Analyze query using SQLGlot optimizers to find improvement opportunities.

        Args:
            expression: The SQL expression to analyze
            analysis: Analysis state to update
            context: Processing context for dialect information
        """
        if not expression:
            return

        original_sql = expression.sql(dialect=context.dialect)
        opportunities = []

        try:
            # Try different SQLGlot optimization strategies
            optimizations = [
                ("join_elimination", eliminate_joins.eliminate_joins, "Eliminate unnecessary joins"),
                ("subquery_elimination", eliminate_subqueries.eliminate_subqueries, "Eliminate or merge subqueries"),
                ("subquery_merging", merge_subqueries.merge_subqueries, "Merge subqueries into main query"),
                (
                    "predicate_pushdown",
                    pushdown_predicates.pushdown_predicates,
                    "Push predicates closer to data sources",
                ),
                (
                    "projection_pushdown",
                    pushdown_projections.pushdown_projections,
                    "Push projections down to reduce data movement",
                ),
                ("join_optimization", optimize_joins.optimize_joins, "Optimize join order and conditions"),
                ("simplification", simplify.simplify, "Simplify expressions and conditions"),
                (
                    "identifier_normalization",
                    normalize_identifiers.normalize_identifiers,
                    "Normalize identifier casing",
                ),
            ]

            best_optimized = expression.copy()
            cumulative_improvement = 0.0

            for opt_type, optimizer, description in optimizations:
                try:
                    # Apply the optimization
                    optimized = optimizer(expression.copy(), dialect=context.dialect)  # type: ignore[operator]

                    if optimized is None:
                        continue

                    optimized_sql = optimized.sql(dialect=context.dialect)

                    # Skip if no changes made
                    if optimized_sql == original_sql:
                        continue

                    # Calculate complexity before and after
                    original_temp_analysis = PerformanceAnalysis()
                    optimized_temp_analysis = PerformanceAnalysis()

                    self._analyze_expression(expression, original_temp_analysis)
                    self._analyze_expression(optimized, optimized_temp_analysis)

                    original_complexity = self._calculate_complexity(original_temp_analysis)
                    optimized_complexity = self._calculate_complexity(optimized_temp_analysis)

                    # Calculate improvement factor
                    if original_complexity > 0:
                        improvement = (original_complexity - optimized_complexity) / original_complexity
                    else:
                        improvement = 0.0

                    # Only add if improvement meets threshold
                    if improvement >= self.config.optimization_threshold:
                        opportunities.append(
                            OptimizationOpportunity(
                                optimization_type=opt_type,
                                description=f"{description} (complexity reduction: {original_complexity - optimized_complexity})",
                                potential_improvement=improvement,
                                complexity_reduction=original_complexity - optimized_complexity,
                                recommendation=f"Apply {opt_type}: {description.lower()}",
                                optimized_sql=optimized_sql,
                            )
                        )

                        # Update the best optimization if this is better
                        if improvement > cumulative_improvement:
                            best_optimized = optimized
                            cumulative_improvement = improvement

                except Exception as e:
                    # Optimization failed, log and continue with next one
                    logger.debug("SQLGlot optimization failed: %s", e)
                    continue

            # Calculate final optimized complexity
            if opportunities:
                optimized_analysis = PerformanceAnalysis()
                self._analyze_expression(best_optimized, optimized_analysis)
                analysis.optimized_complexity = self._calculate_complexity(optimized_analysis)
                analysis.potential_improvement = cumulative_improvement
            else:
                analysis.optimized_complexity = analysis.original_complexity
                analysis.potential_improvement = 0.0

            analysis.optimization_opportunities = opportunities

        except Exception:
            # If optimization analysis fails completely, just skip it
            analysis.optimization_opportunities = []
            analysis.optimized_complexity = analysis.original_complexity
            analysis.potential_improvement = 0.0

    @staticmethod
    def _optimization_to_dict(optimization: OptimizationOpportunity) -> "dict[str, Any]":
        """Convert OptimizationOpportunity to dictionary.

        Args:
            optimization: The optimization opportunity

        Returns:
            Dictionary representation
        """
        return {
            "optimization_type": optimization.optimization_type,
            "description": optimization.description,
            "potential_improvement": optimization.potential_improvement,
            "complexity_reduction": optimization.complexity_reduction,
            "recommendation": optimization.recommendation,
            "optimized_sql": optimization.optimized_sql,
        }

    @staticmethod
    def _issue_to_dict(issue: PerformanceIssue) -> "dict[str, Any]":
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
