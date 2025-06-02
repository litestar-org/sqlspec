"""Validator to detect excessive joins in SQL queries."""

from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("ExcessiveJoins",)

# Constants for excessive joins validation
DEFAULT_MAX_JOINS = 10
"""Default maximum number of joins allowed before flagging as excessive."""

DEFAULT_JOIN_WARN_THRESHOLD = 7
"""Default threshold for join warnings (should be less than max_joins)."""

MAX_CROSS_JOINS_THRESHOLD = 2
"""Maximum number of CROSS JOINs before flagging as risky."""

MAX_SELF_JOINS_THRESHOLD = 3
"""Maximum number of self-joins before flagging as potentially inefficient."""

MAX_NESTED_JOIN_DEPTH = 3
"""Maximum depth of nested subqueries with joins before flagging."""


class ExcessiveJoins(ProcessorProtocol[exp.Expression]):
    """Validator to detect excessive joins in SQL queries.

    Excessive joins can lead to:
    - Poor query performance
    - Cartesian products (unintentional)
    - Complex execution plans
    - Potential security risks from overly complex queries

    Args:
        risk_level: The risk level assigned when excessive joins are detected.
        max_joins: Maximum allowed number of joins before flagging as excessive.
        warn_threshold: Threshold for warnings (should be less than max_joins).
    """

    def __init__(
        self,
        risk_level: RiskLevel = RiskLevel.MEDIUM,
        max_joins: int = DEFAULT_MAX_JOINS,
        warn_threshold: int = DEFAULT_JOIN_WARN_THRESHOLD,
    ) -> None:
        self.risk_level = risk_level
        self.max_joins = max_joins
        self.warn_threshold = warn_threshold

    def process(self, context: "SQLProcessingContext") -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Validate the expression for excessive joins."""
        if context.current_expression is None:
            return exp.Placeholder(), ValidationResult(
                is_safe=False, risk_level=RiskLevel.CRITICAL, issues=["ExcessiveJoins received no expression."]
            )

        expression = context.current_expression

        issues: list[str] = []
        warnings: list[str] = []

        join_counts = self._count_joins(expression)
        total_joins = sum(join_counts.values())

        if total_joins > self.max_joins:
            issues.append(
                f"Excessive joins detected: {total_joins} joins exceed limit of {self.max_joins}. "
                f"Join breakdown: {self._format_join_counts(join_counts)}"
            )
        elif total_joins > self.warn_threshold:
            warnings.append(
                f"High number of joins detected: {total_joins} joins. "
                f"Consider optimizing query structure. Join breakdown: {self._format_join_counts(join_counts)}"
            )

        risky_patterns = self._check_risky_join_patterns(expression, join_counts)
        if risky_patterns:
            if total_joins > self.max_joins:
                issues.extend(risky_patterns)
            else:
                warnings.extend(risky_patterns)

        final_validation_result: Optional[ValidationResult] = None
        if issues:
            final_validation_result = ValidationResult(
                is_safe=False,
                risk_level=self.risk_level,
                issues=issues,
                warnings=warnings,
            )
        else:
            final_validation_result = ValidationResult(
                is_safe=True,
                risk_level=RiskLevel.SAFE if not warnings else RiskLevel.LOW,
                warnings=warnings,
            )

        return context.current_expression, final_validation_result

    def _count_joins(self, expression: exp.Expression) -> dict[str, int]:
        """Count different types of joins in the expression."""
        join_counts: dict[str, int] = {
            "INNER": 0,
            "LEFT": 0,
            "RIGHT": 0,
            "FULL": 0,
            "CROSS": 0,
            "SELF": 0,
            "OTHER": 0,
        }

        # Get all tables referenced in the query for self-join detection
        tables = set()
        for table in expression.find_all(exp.Table):
            if table.this:
                tables.add(str(table.this).lower())

        for join in expression.find_all(exp.Join):
            join_type = self._get_join_type(join)
            join_counts[join_type] += 1

            # Simple heuristic: if same table name appears multiple times, it's likely a self-join
            if hasattr(join, "this") and join.this and len([t for t in tables if t == str(join.this).lower()]) > 1:
                join_counts["SELF"] += 1

        return join_counts

    @staticmethod
    def _get_join_type(join: exp.Join) -> str:
        """Determine the type of join."""
        if join.side and join.side.upper() == "LEFT":
            return "LEFT"
        if join.side and join.side.upper() == "RIGHT":
            return "RIGHT"
        if join.side and join.side.upper() == "FULL":
            return "FULL"
        if join.kind and join.kind.upper() == "CROSS":
            return "CROSS"
        if not join.on and not join.using:  # type: ignore[truthy-function]
            # Join without ON or USING clause is likely a cross join
            return "CROSS"
        return "INNER"

    @staticmethod
    def _format_join_counts(join_counts: dict[str, int]) -> str:
        """Format join counts for display."""
        return ", ".join(f"{k}: {v}" for k, v in {k: v for k, v in join_counts.items() if v > 0}.items())  # lol

    def _check_risky_join_patterns(self, expression: exp.Expression, join_counts: dict[str, int]) -> list[str]:
        """Check for specific risky join patterns."""
        patterns = []

        # Check for excessive cross joins (cartesian products)
        if join_counts["CROSS"] > MAX_CROSS_JOINS_THRESHOLD:
            patterns.append(
                f"Multiple CROSS JOINs detected ({join_counts['CROSS']}). "
                "This may result in cartesian products and poor performance."
            )

        # Check for excessive self-joins
        if join_counts["SELF"] > MAX_SELF_JOINS_THRESHOLD:
            patterns.append(
                f"Multiple self-joins detected ({join_counts['SELF']}). "
                "Consider using window functions or CTEs for better performance."
            )

        # Check for joins without proper conditions
        unconditioned_joins = 0
        for join in expression.find_all(exp.Join):
            if not join.on and not join.using and not (join.kind and join.kind.upper() == "CROSS"):  # type: ignore[truthy-function]
                unconditioned_joins += 1

        if unconditioned_joins > 0:
            patterns.append(
                f"Joins without proper conditions detected ({unconditioned_joins}). "
                "This may result in unintended cartesian products."
            )

        # Check for deeply nested subqueries with joins
        nested_depth = self._check_nested_join_depth(expression)
        if nested_depth > MAX_NESTED_JOIN_DEPTH:
            patterns.append(
                f"Deeply nested subqueries with joins detected (depth: {nested_depth}). "
                "Consider flattening the query structure."
            )

        return patterns

    def _check_nested_join_depth(self, expression: exp.Expression, current_depth: int = 0) -> int:
        """Check the depth of nested subqueries containing joins."""
        max_depth = current_depth

        # Check if current expression has joins
        has_joins = len(list(expression.find_all(exp.Join))) > 0

        # If this level has joins, increment depth
        if has_joins:
            current_depth += 1
            max_depth = max(max_depth, current_depth)

        # Recursively check subqueries
        for subquery in expression.find_all(exp.Subquery):
            if subquery.this:
                nested_depth = self._check_nested_join_depth(subquery.this, current_depth)
                max_depth = max(max_depth, nested_depth)

        return max_depth
