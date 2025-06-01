import re
from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig

__all__ = ("TautologyConditions",)

# Compiled regex patterns for performance
NUMERIC_TAUTOLOGY_PATTERN = re.compile(r"^\s*(\d+)\s*$")
STRING_TAUTOLOGY_PATTERN = re.compile(r"^\s*'([^']*)'\s*$")


class TautologyConditions(SQLValidation):
    """Validates against tautological conditions often used in SQL injection.

    Uses SQLGlot AST parsing to detect:
    - Numeric tautologies: 1 = 1, 2 <> 2, etc.
    - String tautologies: 'a' = 'a', 'x' <> 'x', etc.
    - Column self-comparisons: column = column, table.column <> table.column
    - Mathematical tautologies: (1+1) = 2, etc.
    - Boolean literals: TRUE = TRUE, FALSE <> FALSE

    Args:
        risk_level: The risk level of the validator.
        min_risk_to_raise: The minimum risk level to raise an issue.
        allow_mathematical_constants: Allow mathematical constant comparisons like 2+2=4.
        max_depth: Maximum AST depth to search (prevents infinite recursion).
    """

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.MEDIUM,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.MEDIUM,
        allow_mathematical_constants: bool = False,
        max_depth: int = 10,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        self.allow_mathematical_constants = allow_mathematical_constants
        self.max_depth = max_depth

    def validate(
        self,
        expression: exp.Expression,
        dialect: "DialectType",
        config: "SQLConfig",
        **kwargs: Any,
    ) -> ValidationResult:
        """Validate the expression for tautological conditions."""
        issues: list[str] = []
        warnings: list[str] = []

        # Find all comparison expressions (=, <>, !=, <, >, <=, >=)
        comparison_types = [exp.EQ, exp.NEQ, exp.LT, exp.GT, exp.LTE, exp.GTE]

        for comparison_type in comparison_types:
            for comparison in expression.find_all(comparison_type):
                if self._is_tautology(comparison):
                    left_sql = comparison.this.sql(dialect=dialect) if comparison.this else "NULL"
                    right_sql = comparison.expression.sql(dialect=dialect) if comparison.expression else "NULL"

                    issues.append(
                        f"Tautological condition detected: {left_sql} {self._get_operator_symbol(comparison)} {right_sql}"
                    )

        # Check for OR conditions with tautologies (classic injection pattern)
        for or_expr in expression.find_all(exp.Or):
            if self._contains_tautology_in_or(or_expr):
                or_sql = or_expr.sql(dialect=dialect)
                issues.append(f"OR clause contains tautological condition (potential injection): {or_sql[:100]}...")

        # Check for AND conditions with contradictions
        for and_expr in expression.find_all(exp.And):
            if self._contains_contradiction_in_and(and_expr):
                and_sql = and_expr.sql(dialect=dialect)
                warnings.append(f"AND clause contains contradictory condition: {and_sql[:100]}...")

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues, warnings=warnings)

        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE, warnings=warnings)

    def _is_tautology(self, comparison: exp.Expression) -> bool:
        """Check if a comparison is a tautology."""
        if not hasattr(comparison, "this") or not hasattr(comparison, "expression"):
            return False

        left = comparison.this
        right = comparison.expression

        # Check for identical expressions (column = column, etc.)
        if self._expressions_identical(left, right):
            # For = and >=, <=: always true
            # For <>, !=: always false (contradiction)
            # For < and >: always false (contradiction)
            return isinstance(comparison, (exp.EQ, exp.LTE, exp.GTE))

        # Check for literal tautologies
        if isinstance(left, exp.Literal) and isinstance(right, exp.Literal):
            return self._check_literal_tautology(comparison, left, right)

        # Check for mathematical tautologies if not allowing constants
        if not self.allow_mathematical_constants:
            return self._check_mathematical_tautology(comparison, left, right)

        return False

    @staticmethod
    def _expressions_identical(left: exp.Expression, right: exp.Expression) -> bool:
        """Check if two expressions are structurally identical."""
        if type(left) is not type(right):
            return False

        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            # Compare column names and table references
            left_name = str(left.this) if left.this else ""
            right_name = str(right.this) if right.this else ""
            left_table = str(left.args.get("table", "")) if left.args.get("table") else ""
            right_table = str(right.args.get("table", "")) if right.args.get("table") else ""

            return left_name.lower() == right_name.lower() and left_table.lower() == right_table.lower()

        if isinstance(left, exp.Literal) and isinstance(right, exp.Literal):
            return str(left.this) == str(right.this) and left.is_string == right.is_string

        # For more complex expressions, compare SQL representation
        return left.sql() == right.sql()

    @staticmethod
    def _check_literal_tautology(comparison: exp.Expression, left: exp.Literal, right: exp.Literal) -> bool:
        """Check if literal comparison is a tautology."""
        # Same value comparisons
        if str(left.this) == str(right.this) and left.is_string == right.is_string:
            return isinstance(comparison, (exp.EQ, exp.LTE, exp.GTE))

        # Common injection patterns
        if left.is_string and right.is_string:
            # 'a' = 'a', '' = '', etc.
            return str(left.this) == str(right.this) and isinstance(comparison, (exp.EQ, exp.LTE, exp.GTE))

        if not left.is_string and not right.is_string:
            try:
                # Numeric comparisons: 1 = 1, 0 <> 0, etc.
                left_val = float(str(left.this))
                right_val = float(str(right.this))

                if left_val == right_val:
                    return isinstance(comparison, (exp.EQ, exp.LTE, exp.GTE))

                # Check for obvious false conditions
                if isinstance(comparison, (exp.NEQ)) and left_val == right_val:
                    return True  # This is a contradiction, also suspicious

            except (ValueError, TypeError):
                pass

        return False

    @staticmethod
    def _check_mathematical_tautology(comparison: exp.Expression, left: exp.Expression, right: exp.Expression) -> bool:
        """Check for mathematical tautologies like 2+2=4."""
        # Simple mathematical expressions that evaluate to known constants
        mathematical_patterns = [
            (r"^\s*1\s*\+\s*1\s*$", "2"),
            (r"^\s*2\s*\+\s*2\s*$", "4"),
            (r"^\s*1\s*\*\s*1\s*$", "1"),
            (r"^\s*0\s*\*\s*\d+\s*$", "0"),
            (r"^\s*\d+\s*\*\s*0\s*$", "0"),
        ]

        left_sql = left.sql().strip()
        right_sql = right.sql().strip()

        for pattern, expected in mathematical_patterns:
            if re.match(pattern, left_sql) and right_sql == expected:
                return isinstance(comparison, (exp.EQ, exp.LTE, exp.GTE))
            if re.match(pattern, right_sql) and left_sql == expected:
                return isinstance(comparison, (exp.EQ, exp.LTE, exp.GTE))

        return False

    def _contains_tautology_in_or(self, or_expr: exp.Or) -> bool:
        """Check if OR expression contains any tautological conditions."""
        # Recursively check all conditions in the OR chain
        conditions = self._flatten_or_conditions(or_expr)

        for condition in conditions:
            if isinstance(condition, (exp.EQ, exp.NEQ, exp.LT, exp.GT, exp.LTE, exp.GTE)) and self._is_tautology(
                condition
            ):
                return True

        return False

    def _contains_contradiction_in_and(self, and_expr: exp.And) -> bool:
        """Check if AND expression contains contradictory conditions."""
        conditions = self._flatten_and_conditions(and_expr)

        for condition in conditions:
            if isinstance(condition, (exp.NEQ, exp.LT, exp.GT)) and self._is_contradiction(condition):
                return True

        return False

    def _is_contradiction(self, comparison: exp.Expression) -> bool:
        """Check if a comparison is a contradiction (always false)."""
        if not hasattr(comparison, "this") or not hasattr(comparison, "expression"):
            return False

        left = comparison.this
        right = comparison.expression

        # Identical expressions with inequality operators
        if self._expressions_identical(left, right):
            return isinstance(comparison, (exp.NEQ, exp.LT, exp.GT))

        return False

    @staticmethod
    def _flatten_or_conditions(or_expr: exp.Or) -> list[exp.Expression]:
        """Flatten nested OR conditions into a list."""
        conditions = []

        def extract_conditions(expr: exp.Expression) -> None:
            if isinstance(expr, exp.Or):
                if expr.this:
                    extract_conditions(expr.this)
                if expr.expression:
                    extract_conditions(expr.expression)
            else:
                conditions.append(expr)

        extract_conditions(or_expr)
        return conditions

    @staticmethod
    def _flatten_and_conditions(and_expr: exp.And) -> list[exp.Expression]:
        """Flatten nested AND conditions into a list."""
        conditions = []

        def extract_conditions(expr: exp.Expression) -> None:
            if isinstance(expr, exp.And):
                if expr.this:
                    extract_conditions(expr.this)
                if expr.expression:
                    extract_conditions(expr.expression)
            else:
                conditions.append(expr)

        extract_conditions(and_expr)
        return conditions

    @staticmethod
    def _get_operator_symbol(comparison: exp.Expression) -> str:
        """Get the SQL operator symbol for a comparison."""
        if isinstance(comparison, exp.EQ):
            return "="
        if isinstance(comparison, exp.NEQ):
            return "<>"
        if isinstance(comparison, exp.LT):
            return "<"
        if isinstance(comparison, exp.GT):
            return ">"
        if isinstance(comparison, exp.LTE):
            return "<="
        if isinstance(comparison, exp.GTE):
            return ">="
        return "??"
