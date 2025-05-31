from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.base import SQLValidation, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig


class PreventDDL(SQLValidation):
    """Validates against the presence of DDL statements."""

    def __init__(
        self,
        risk_level: "RiskLevel" = RiskLevel.HIGH,
        min_risk_to_raise: "Optional[RiskLevel]" = RiskLevel.HIGH,
        allowed_ddl_kinds: "Optional[list[str]]" = None,  # e.g., ["TABLE", "VIEW"]
        banned_ddl_expressions: "Optional[list[type[exp.Expression]]]" = None,
    ) -> None:
        super().__init__(risk_level, min_risk_to_raise)
        self.allowed_ddl_kinds = [kind.upper() for kind in allowed_ddl_kinds] if allowed_ddl_kinds else []
        self.banned_ddl_expressions = banned_ddl_expressions or [
            exp.Create,
            exp.Command,
            exp.Drop,
            exp.TruncateTable,
        ]

    def validate(
        self,
        expression: "exp.Expression",
        dialect: "DialectType",
        config: "SQLConfig",
        **kwargs: "Any",
    ) -> "ValidationResult":
        issues = []

        for ddl_expr_type in self.banned_ddl_expressions:
            for node in expression.find_all(ddl_expr_type):
                # For CREATE statements, we can check the 'kind' (TABLE, VIEW, INDEX, etc.)
                if isinstance(node, exp.Create):
                    kind = node.args.get("kind")
                    if kind and isinstance(kind, str) and kind.upper() in self.allowed_ddl_kinds:
                        continue  # This specific kind of CREATE is allowed
                    issues.append(f"Disallowed DDL statement found: {type(node).__name__} (Kind: {kind or 'N/A'})")
                elif isinstance(node, exp.Drop):
                    kind = node.args.get("kind")  # e.g. TABLE, INDEX
                    exists = node.args.get("exists")  # IF EXISTS
                    issues.append(
                        f"Disallowed DDL statement found: {type(node).__name__} (Kind: {kind or 'N/A'}, Exists: {exists})"
                    )
                elif isinstance(node, exp.Command):
                    command_verb = str(node.this).upper()
                    if command_verb == "ALTER":
                        # Could add more detailed checks for specific ALTER actions if needed
                        # For example, inspect node.kind for ALTER TABLE, ALTER VIEW etc.
                        issues.append(f"Disallowed DDL statement found: ALTER Command ({node.sql(dialect=dialect)})")
                    # Add other command verbs here if necessary
                elif isinstance(node, exp.TruncateTable):
                    issues.append(f"Disallowed DDL statement found: {type(node).__name__}")
                else:
                    issues.append(f"Disallowed DDL statement found: {type(node).__name__}")

        if issues:
            return ValidationResult(is_safe=False, risk_level=self.risk_level, issues=issues)
        return ValidationResult(is_safe=True, risk_level=RiskLevel.SAFE)
