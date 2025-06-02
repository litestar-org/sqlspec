# ruff: noqa: ARG004
"""Validator to detect cartesian products in SQL queries."""

from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("CartesianProductDetector",)

# Constants for cartesian product detection
DEFAULT_MAX_TABLE_PRODUCT_SIZE = 1000000
"""Default maximum table product size (1M rows) before flagging cartesian products."""

MIN_TABLES_FOR_CARTESIAN_CHECK = 2
"""Minimum number of tables required before checking for cartesian products."""

CARTESIAN_SAFETY_MARGIN = 1
"""Safety margin for cartesian product calculations."""


class CartesianProductDetector(ProcessorProtocol[exp.Expression]):
    """Validator to detect potential cartesian products in SQL queries.

    Cartesian products occur when:
    - Tables are joined without proper join conditions
    - CROSS JOINs are used (intentionally or unintentionally)
    - Multiple tables are listed in FROM clause without WHERE conditions
    - Incorrect join conditions lead to many-to-many relationships

    These can lead to:
    - Massive result sets causing performance issues
    - Memory exhaustion
    - Potential DoS attacks
    - Unintended data exposure

    Args:
        risk_level: The risk level assigned when cartesian products are detected.
        allow_explicit_cross_joins: Whether to allow explicit CROSS JOINs.
        max_table_product_size: Maximum allowed estimated table product size.
    """

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.HIGH,
        allow_explicit_cross_joins: bool = True,
        max_table_product_size: int = DEFAULT_MAX_TABLE_PRODUCT_SIZE,
    ) -> None:
        self.risk_level = risk_level
        self.allow_explicit_cross_joins = allow_explicit_cross_joins
        self.max_table_product_size = max_table_product_size

    def process(self, context: "SQLProcessingContext") -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Validate the expression for cartesian product patterns."""
        if context.current_expression is None:
            return exp.Placeholder(), ValidationResult(
                is_safe=False,
                risk_level=RiskLevel.CRITICAL,
                issues=["CartesianProductDetector received no expression."],
            )

        expression = context.current_expression

        issues: list[str] = []
        warnings: list[str] = []

        # Check different types of cartesian product risks
        self._check_explicit_cross_joins(expression, issues, warnings)
        self._check_implicit_cartesian_products(expression, issues, warnings)
        self._check_missing_join_conditions(expression, issues, warnings)
        self._check_comma_separated_tables(expression, issues, warnings)
        self._check_subquery_cartesian_risks(expression, issues, warnings)

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

    def _check_explicit_cross_joins(self, expression: "exp.Expression", issues: list[str], warnings: list[str]) -> None:
        """Check for explicit CROSS JOINs."""
        cross_joins = [join for join in expression.find_all(exp.Join) if join.kind and join.kind.upper() == "CROSS"]

        if cross_joins:
            if not self.allow_explicit_cross_joins:
                issues.append(
                    f"Explicit CROSS JOIN detected ({len(cross_joins)} occurrences). "
                    "This will create a cartesian product."
                )
            else:
                warnings.append(
                    f"Explicit CROSS JOIN detected ({len(cross_joins)} occurrences). "
                    "Ensure this is intentional as it creates cartesian products."
                )

    def _check_implicit_cartesian_products(
        self, expression: "exp.Expression", issues: list[str], warnings: list[str]
    ) -> None:
        """Check for implicit cartesian products from joins without conditions."""
        for join in expression.find_all(exp.Join):
            # Skip explicitly allowed cross joins
            if join.kind and join.kind.upper() == "CROSS" and self.allow_explicit_cross_joins:
                continue

            # Check if join lacks ON or USING clause
            on_condition = join.args.get("on")
            using_clause = join.args.get("using")
            if not on_condition and not using_clause:
                table_name = self._get_table_name(join.this) if join.this else "unknown"
                issues.append(
                    f"JOIN without ON or USING clause detected for table '{table_name}'. "
                    "This creates an implicit cartesian product."
                )

    def _check_missing_join_conditions(
        self, expression: "exp.Expression", issues: list[str], warnings: list[str]
    ) -> None:
        """Check for potentially insufficient join conditions."""
        for join in expression.find_all(exp.Join):
            condition = join.args.get("on")
            if condition:
                join_condition_analysis = self._analyze_join_condition(condition)

                if join_condition_analysis["has_literals_only"]:
                    issues.append(
                        "JOIN condition contains only literals (no column references). "
                        "This may result in a cartesian product."
                    )

                if join_condition_analysis["has_constant_true"]:
                    issues.append("JOIN condition is always TRUE. This creates a cartesian product.")

                if join_condition_analysis["has_inequality_only"]:
                    warnings.append(
                        "JOIN condition uses only inequality operators. This may result in large result sets."
                    )

    def _check_comma_separated_tables(
        self, expression: "exp.Expression", issues: list[str], warnings: list[str]
    ) -> None:
        """Check for comma-separated tables in FROM clause without proper WHERE conditions."""
        # This is trickier to detect with SQLGlot as it usually parses comma-separated tables
        # as implicit joins. We'll check for multiple tables in FROM with weak WHERE conditions.

        for select in expression.find_all(exp.Select):
            # Get the actual FROM clause object, not the method
            from_clause = select.args.get("from")
            if not from_clause:
                continue

            # Count tables in FROM clause
            tables = self._get_tables_from_from_clause(from_clause)

            if len(tables) > MIN_TABLES_FOR_CARTESIAN_CHECK:
                # Multiple tables - check if WHERE clause has proper join conditions
                where_clause = select.find(exp.Where)

                if not where_clause:
                    issues.append(
                        f"Multiple tables ({len(tables)}) in FROM clause without WHERE clause. "
                        f"Tables: {', '.join(tables)}. This creates a cartesian product."
                    )
                else:
                    # Check if WHERE clause has table correlations
                    has_table_correlation = self._check_where_for_table_correlation(where_clause, tables)
                    if not has_table_correlation:
                        warnings.append(
                            f"Multiple tables ({len(tables)}) in FROM clause but WHERE clause "
                            "may not properly correlate them. "
                            f"Tables: {', '.join(tables)}."
                        )

    def _check_subquery_cartesian_risks(
        self, expression: "exp.Expression", issues: list[str], warnings: list[str]
    ) -> None:
        """Check for cartesian product risks in subqueries."""
        for subquery in expression.find_all(exp.Subquery):
            if subquery.this:
                # Recursively check subqueries
                sub_issues: list[str] = []
                sub_warnings: list[str] = []

                self._check_explicit_cross_joins(subquery.this, sub_issues, sub_warnings)
                self._check_implicit_cartesian_products(subquery.this, sub_issues, sub_warnings)
                self._check_missing_join_conditions(subquery.this, sub_issues, sub_warnings)

                # Add context that these are in subqueries
                issues.extend(f"In subquery: {issue}" for issue in sub_issues)
                warnings.extend(f"In subquery: {warning}" for warning in sub_warnings)

    @staticmethod
    def _get_table_name(table_expr: "exp.Expression") -> str:
        """Extract table name from table expression."""
        if isinstance(table_expr, exp.Table):
            return str(table_expr.this) if table_expr.this else "unknown"
        if hasattr(table_expr, "this"):
            return str(table_expr.this)
        return str(table_expr)

    @staticmethod
    def _analyze_join_condition(condition: Any) -> dict[str, bool]:
        """Analyze a join condition for potential issues."""
        analysis = {
            "has_literals_only": True,
            "has_constant_true": False,
            "has_inequality_only": True,
            "has_column_references": False,
        }

        if not condition or not isinstance(condition, exp.Expression):
            return analysis

        # Check for column references
        columns = list(condition.find_all(exp.Column))
        if columns:
            analysis["has_literals_only"] = False
            analysis["has_column_references"] = True

        # Check for constant TRUE conditions
        if isinstance(condition, exp.Boolean) and condition.this:
            analysis["has_constant_true"] = True

        # Check for literal values in conditions
        literals = list(condition.find_all(exp.Literal))
        if literals and not columns:
            analysis["has_literals_only"] = True

        # Check for equality operators
        equalities = list(condition.find_all(exp.EQ))
        if equalities:
            analysis["has_inequality_only"] = False

        return analysis

    @staticmethod
    def _get_tables_from_from_clause(from_clause: exp.From) -> list[str]:
        """Extract table names from FROM clause."""

        if not from_clause:
            return []
        return [str(table.this) for table in from_clause.find_all(exp.Table) if table.this]

    @staticmethod
    def _check_where_for_table_correlation(where_clause: exp.Where, tables: list[str]) -> bool:
        """Check if WHERE clause properly correlates tables."""
        if not where_clause.this:
            return False

        # Look for column references that include table qualifiers
        columns = list(where_clause.find_all(exp.Column))

        # Check if we have columns from multiple tables
        table_refs = set()
        for column in columns:
            if column.table:
                table_refs.add(str(column.table).lower())

        # If we have references to multiple tables, consider it correlated
        return len(table_refs) > 1

    def _estimate_result_size(self, expression: exp.Expression) -> int:
        """Estimate the potential result size (placeholder for future enhancement)."""
        # This is a placeholder for more sophisticated analysis
        # In a real implementation, this might use statistics or heuristics
        table_count = len(list(expression.find_all(exp.Table)))
        join_count = len(list(expression.find_all(exp.Join)))

        # Simple heuristic: more tables + fewer proper joins = higher risk
        if table_count > MIN_TABLES_FOR_CARTESIAN_CHECK and join_count < table_count - CARTESIAN_SAFETY_MARGIN:
            return self.max_table_product_size + CARTESIAN_SAFETY_MARGIN  # Exceed threshold

        return 0  # Safe estimate
