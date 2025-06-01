"""Validator to detect excessive joins in SQL queries."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.pipelines.base import AnalysisResult
    from sqlspec.statement.sql import SQLConfig

__all__ = ("ExcessiveJoins",)


class ExcessiveJoins(SQLValidation):
    """Validator to detect excessive joins in SQL queries.

    Excessive joins can lead to:
    - Poor query performance
    - Cartesian products (unintentional)
    - Complex execution plans
    - Potential security risks from overly complex queries

    Args:
        risk_level: The risk level assigned when excessive joins are detected.
        min_risk_to_raise: Minimum risk level to raise an issue.
        max_joins: Maximum allowed number of joins before flagging as excessive.
        warn_threshold: Threshold for warnings (should be less than max_joins).
    """

    def __init__(
        self,
        risk_level: RiskLevel = RiskLevel.MEDIUM,
        min_risk_to_raise: RiskLevel | None = RiskLevel.MEDIUM,
        max_joins: int = 10,
        warn_threshold: int = 7,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        self.max_joins = max_joins
        self.warn_threshold = warn_threshold

    def validate(
        self,
        expression: exp.Expression,
        dialect: DialectType,
        config: SQLConfig,
        **kwargs: Any,
    ) -> ValidationResult:
        """Validate the expression for excessive joins."""
        issues: list[str] = []
        warnings: list[str] = []

        # Count different types of joins
        join_counts = self._count_joins(expression)
        total_joins = sum(join_counts.values())

        # Check for excessive joins
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

        # Check for specific risky join patterns
        risky_patterns = self._check_risky_join_patterns(expression, join_counts)
        if risky_patterns:
            if total_joins > self.max_joins:
                issues.extend(risky_patterns)
            else:
                warnings.extend(risky_patterns)

        # Determine final result
        if issues:
            return ValidationResult(
                is_safe=False,
                risk_level=self.risk_level,
                issues=issues,
                warnings=warnings,
            )

        return ValidationResult(
            is_safe=True,
            risk_level=RiskLevel.SAFE if not warnings else RiskLevel.LOW,
            warnings=warnings,
        )

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

            # Check for self-joins
            if hasattr(join, "this") and join.this:
                join_table = str(join.this).lower() if hasattr(join.this, "this") else str(join.this).lower()
                # Simple heuristic: if same table name appears multiple times, it's likely a self-join
                if len([t for t in tables if t == join_table]) > 1:
                    join_counts["SELF"] += 1

        return join_counts

    def _get_join_type(self, join: exp.Join) -> str:
        """Determine the type of join."""
        if join.side and join.side.upper() == "LEFT":
            return "LEFT"
        if join.side and join.side.upper() == "RIGHT":
            return "RIGHT"
        if join.side and join.side.upper() == "FULL":
            return "FULL"
        if join.kind and join.kind.upper() == "CROSS":
            return "CROSS"
        if not join.on and not join.using:
            # Join without ON or USING clause is likely a cross join
            return "CROSS"
        return "INNER"

    def _format_join_counts(self, join_counts: dict[str, int]) -> str:
        """Format join counts for display."""
        active_joins = {k: v for k, v in join_counts.items() if v > 0}
        return ", ".join(f"{k}: {v}" for k, v in active_joins.items())

    def _check_risky_join_patterns(self, expression: exp.Expression, join_counts: dict[str, int]) -> list[str]:
        """Check for specific risky join patterns."""
        patterns = []

        # Check for excessive cross joins (cartesian products)
        if join_counts["CROSS"] > 2:
            patterns.append(
                f"Multiple CROSS JOINs detected ({join_counts['CROSS']}). "
                "This may result in cartesian products and poor performance."
            )

        # Check for excessive self-joins
        if join_counts["SELF"] > 3:
            patterns.append(
                f"Multiple self-joins detected ({join_counts['SELF']}). "
                "Consider using window functions or CTEs for better performance."
            )

        # Check for joins without proper conditions
        unconditioned_joins = 0
        for join in expression.find_all(exp.Join):
            if not join.on and not join.using and not (join.kind and join.kind.upper() == "CROSS"):
                unconditioned_joins += 1

        if unconditioned_joins > 0:
            patterns.append(
                f"Joins without proper conditions detected ({unconditioned_joins}). "
                "This may result in unintended cartesian products."
            )

        # Check for deeply nested subqueries with joins
        nested_depth = self._check_nested_join_depth(expression)
        if nested_depth > 3:
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

    def validate_with_analysis(
        self,
        expression: exp.Expression,
        analysis: AnalysisResult,
        dialect: DialectType,
        config: SQLConfig,
        **kwargs: Any,
    ) -> ValidationResult:
        """Validate using pre-computed analysis results for efficiency.

        Args:
            expression: The SQL expression to validate
            analysis: Pre-computed analysis results
            dialect: The SQL dialect
            config: The SQL configuration
            kwargs: Additional keyword arguments

        Returns:
            ValidationResult with join-related issues and warnings
        """
        issues: list[str] = []
        warnings: list[str] = []

        # Use pre-computed join analysis
        join_count = analysis.metrics.get("join_count", 0)
        join_types = analysis.metrics.get("join_types", {})
        cartesian_risk = analysis.metrics.get("cartesian_risk", 0)

        # Check for excessive joins
        if join_count > self.max_joins:
            issues.append(
                f"Excessive joins detected: {join_count} joins exceed limit of {self.max_joins}. "
                f"Join breakdown: {self._format_join_counts(join_types)}"
            )
        elif join_count > self.warn_threshold:
            warnings.append(
                f"High number of joins detected: {join_count} joins. "
                f"Consider optimizing query structure. Join breakdown: {self._format_join_counts(join_types)}"
            )

        # Check for cartesian product risks
        if cartesian_risk > 0:
            if join_count > self.max_joins:
                issues.append(f"Cartesian product risk detected: {cartesian_risk} risky join patterns")
            else:
                warnings.append(f"Potential cartesian product risk: {cartesian_risk} risky join patterns")

        # Determine final result
        if issues:
            return ValidationResult(
                is_safe=False,
                risk_level=self.risk_level,
                issues=issues,
                warnings=warnings,
            )

        return ValidationResult(
            is_safe=True,
            risk_level=RiskLevel.SAFE if not warnings else RiskLevel.LOW,
            warnings=warnings,
        )
