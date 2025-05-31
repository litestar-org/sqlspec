from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig


class RiskyDML(SQLValidation):
    """Validates DML statements for risky operations.

    This validator checks for DELETE and UPDATE statements without a WHERE clause.

    Args:
        risk_level: The risk level of the validator.
        min_risk_to_raise: The minimum risk level to raise an issue.
        allow_delete_without_where: Whether to allow DELETE statements without a WHERE clause.
        allow_update_without_where: Whether to allow UPDATE statements without a WHERE clause.



    """

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.MEDIUM,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.HIGH,  # Only raise if very severe by default
        allow_delete_without_where: bool = False,
        allow_update_without_where: bool = False,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        self.allow_delete_without_where = allow_delete_without_where
        self.allow_update_without_where = allow_update_without_where

    def validate(
        self,
        expression: "exp.Expression",
        dialect: "DialectType",
        config: "SQLConfig",
        **kwargs: "Any",
    ) -> "ValidationResult":
        issues: list[str] = []

        # Check for DELETE statements without a WHERE clause
        if not self.allow_delete_without_where:
            issues.extend(
                "Risky DML: DELETE statement found without a WHERE clause. This will delete all rows in the table."
                for delete_expr in expression.find_all(exp.Delete)
                if not delete_expr.args.get("where")
            )

        # Check for UPDATE statements without a WHERE clause
        if not self.allow_update_without_where:
            issues.extend(
                "Risky DML: UPDATE statement found without a WHERE clause. This will update all rows in the table."
                for update_expr in expression.find_all(exp.Update)
                if not update_expr.args.get("where")
            )

        # Could add checks for TRUNCATE if it's parsed as a DML by sqlglot in some contexts,
        # or handle it in DDLValidator if it's considered DDL.
        # For sqlglot, TRUNCATE is often its own expression type (exp.TruncateTable)

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues)
        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
