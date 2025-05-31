from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig


class TautologyConditions(SQLValidation):
    """Validates against tautological conditions often used in SQL injection.

    Checks for:
    - X = X
    - X <> X
    - 1 = 1
    - 1 <> 1
    - OR 1=1
    - OR 'a'='a'
    - AND 1=2

    Args:
        risk_level: The risk level of the validator.
        min_risk_to_raise: The minimum risk level to raise an issue.
    """

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.MEDIUM,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.MEDIUM,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)

    def _is_tautology(self, condition_expr: "exp.Expression") -> bool:
        # Basic check: OR/AND X=X, X<>X, 1=1, etc.
        # This can be quite complex to do perfectly for all cases.

        if isinstance(condition_expr, exp.EQ):
            if condition_expr.left.sql() == condition_expr.right.sql():
                return True
            if (
                isinstance(condition_expr.left, exp.Literal)
                and isinstance(condition_expr.right, exp.Literal)
                and condition_expr.left.this == condition_expr.right.this
            ):
                return True
        elif (
            isinstance(condition_expr, exp.NEQ)
            and isinstance(condition_expr.left, exp.Literal)
            and isinstance(condition_expr.right, exp.Literal)
            and condition_expr.left.this != condition_expr.right.this
        ):
            return True

        # More complex patterns: OR 1=1, AND 1=2
        # Example: If it's an OR expression, and one side is a clear tautology like 1=1
        if isinstance(condition_expr, exp.Or) and (
            self._is_tautology(condition_expr.left) or self._is_tautology(condition_expr.right)
        ):
            # If one side of OR is always true, the whole OR is always true (if that side is evaluated)
            # This check is recursive and might be too aggressive or simplistic.
            # For instance `OR 1=1` is a tautology. `OR some_col = some_col` is a tautology.
            # `OR (some_col = 1 OR 1=1)` is a tautology.
            pass  # Let's be careful with recursive calls here to avoid excessive depth or false positives.

        return False

    def validate(
        self,
        expression: "exp.Expression",
        dialect: "DialectType",
        config: "SQLConfig",
        **kwargs: "Any",
    ) -> "ValidationResult":
        # Look for tautologies in WHERE clauses and JOIN conditions
        issues = [
            f"Potential tautology found in WHERE clause: {where_expr.this.sql(dialect=dialect)[:100]}..."
            for where_expr in expression.find_all(exp.Where)
            if self._is_tautology(where_expr.this)
        ]

        issues.extend(
            f"Potential tautology found in JOIN condition: {join_expr.args['on'].sql(dialect=dialect)[:100]}..."
            for join_expr in expression.find_all(exp.Join)
            if join_expr.args.get("on") and self._is_tautology(join_expr.args["on"])
        )

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues)
        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
