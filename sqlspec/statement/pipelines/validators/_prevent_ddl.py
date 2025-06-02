from typing import TYPE_CHECKING, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("PreventDDL",)


class PreventDDL(ProcessorProtocol[exp.Expression]):
    """Validates against the presence of DDL statements, using the processing context."""

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.HIGH,
        allowed_ddl_kinds: "Optional[list[str]]" = None,  # e.g., ["TABLE", "VIEW"]
        banned_ddl_expressions: "Optional[list[type[exp.Expression]]]" = None,
    ) -> None:
        self.risk_level = risk_level
        self.allowed_ddl_kinds = [kind.upper() for kind in allowed_ddl_kinds] if allowed_ddl_kinds else []
        self.banned_ddl_expressions = banned_ddl_expressions or [
            exp.Create,
            exp.Command,
            exp.Drop,
            exp.TruncateTable,
        ]

    def process(self, context: "SQLProcessingContext") -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Validate the expression in the context against DDL statements."""
        if context.current_expression is None:
            return exp.Placeholder(), ValidationResult(
                is_safe=False,
                risk_level=RiskLevel.CRITICAL,
                issues=["PreventDDL received no expression for validation."],
            )

        issues = []
        expression_to_validate = context.current_expression
        dialect_to_use = context.dialect

        for ddl_expr_type in self.banned_ddl_expressions:
            for node in expression_to_validate.find_all(ddl_expr_type):
                if isinstance(node, exp.Create):
                    kind = node.args.get("kind")
                    if kind and isinstance(kind, str) and kind.upper() in self.allowed_ddl_kinds:
                        continue
                    issues.append(f"Disallowed DDL statement found: {type(node).__name__} (Kind: {kind or 'N/A'})")
                elif isinstance(node, exp.Drop):
                    kind = node.args.get("kind")
                    exists = node.args.get("exists")
                    issues.append(
                        f"Disallowed DDL statement found: {type(node).__name__} (Kind: {kind or 'N/A'}, Exists: {exists})"
                    )
                elif isinstance(node, exp.Command):
                    command_verb = str(node.this).upper()
                    if command_verb == "ALTER":
                        issues.append(
                            f"Disallowed DDL statement found: ALTER Command ({node.sql(dialect=dialect_to_use)})"
                        )
                elif isinstance(node, exp.TruncateTable):
                    issues.append(f"Disallowed DDL statement found: {type(node).__name__}")
                else:
                    issues.append(f"Disallowed DDL statement found: {type(node).__name__}")

        if issues:
            return context.current_expression, ValidationResult(
                is_safe=False, risk_level=self.risk_level, issues=issues
            )
        return context.current_expression, ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
