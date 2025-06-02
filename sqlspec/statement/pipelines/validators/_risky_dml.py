from typing import Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult
from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("RiskyDML",)


class RiskyDML(ProcessorProtocol[exp.Expression]):
    """Validates DML statements for risky operations.

    This validator checks for DELETE and UPDATE statements without a WHERE clause.

    Args:
        risk_level: The risk level of the validator.
        allow_delete_without_where: Whether to allow DELETE statements without a WHERE clause.
        allow_update_without_where: Whether to allow UPDATE statements without a WHERE clause.
    """

    def __init__(
        self,
        risk_level: RiskLevel = RiskLevel.MEDIUM,
        allow_delete_without_where: bool = False,
        allow_update_without_where: bool = False,
    ) -> None:
        self.risk_level = risk_level
        self.allow_delete_without_where = allow_delete_without_where
        self.allow_update_without_where = allow_update_without_where

    def process(self, context: SQLProcessingContext) -> tuple[exp.Expression, Optional[ValidationResult]]:
        """Validate the expression for risky DML operations."""
        if context.current_expression is None:
            return exp.Placeholder(), ValidationResult(
                is_safe=False, risk_level=RiskLevel.CRITICAL, issues=["RiskyDML received no expression."]
            )

        expression = context.current_expression

        issues: list[str] = []

        if not self.allow_delete_without_where:
            issues.extend(
                "Risky DML: DELETE statement found without a WHERE clause. This will delete all rows in the table."
                for delete_expr in expression.find_all(exp.Delete)
                if not delete_expr.args.get("where")
            )

        if not self.allow_update_without_where:
            issues.extend(
                "Risky DML: UPDATE statement found without a WHERE clause. This will update all rows in the table."
                for update_expr in expression.find_all(exp.Update)
                if not update_expr.args.get("where")
            )

        final_validation_result: Optional[ValidationResult] = None
        if issues:
            final_validation_result = ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues)
        else:
            final_validation_result = ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)

        return context.current_expression, final_validation_result
