"""DML Safety Validator"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from sqlglot import expressions as exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement_new.protocols import ProcessorPhase, SQLProcessingContext, SQLProcessor, ValidationError

__all__ = ("DMLSafetyConfig", "DMLSafetyValidator", "StatementCategory")

class StatementCategory(Enum):
    """Categories for SQL statement types."""
    DDL = "ddl"
    DML = "dml"
    DQL = "dql"
    DCL = "dcl"
    TCL = "tcl"

@dataclass
class DMLSafetyConfig:
    """Configuration for DML safety validation."""
    prevent_ddl: bool = True
    prevent_dcl: bool = True
    require_where_clause: "set[str]" = field(default_factory=lambda: {"DELETE", "UPDATE"})
    allowed_ddl_operations: "set[str]" = field(default_factory=set)
    migration_mode: bool = False
    max_affected_rows: "Optional[int]" = None

class DMLSafetyValidator(SQLProcessor):
    """Unified validator for DML/DDL safety checks."""
    phase = ProcessorPhase.VALIDATE

    def __init__(self, config: "Optional[DMLSafetyConfig]" = None) -> None:
        self.config = config or DMLSafetyConfig()

    def process(self, context: "SQLProcessingContext") -> "SQLProcessingContext":
        if context.current_expression is None:
            return context
        self.validate(context.current_expression, context)
        return context

    def validate(self, expression: "exp.Expression", context: "SQLProcessingContext") -> None:
        category = self._categorize_statement(expression)
        operation = self._get_operation_type(expression)

        if category == StatementCategory.DDL and self.config.prevent_ddl:
            if operation not in self.config.allowed_ddl_operations:
                self.add_error(
                    context,
                    message=f"DDL operation '{operation}' is not allowed",
                    code="ddl-not-allowed",
                    risk_level=RiskLevel.CRITICAL,
                    expression=expression,
                )
        elif category == StatementCategory.DML and operation in self.config.require_where_clause and not self._has_where_clause(expression):
            self.add_error(
                context,
                message=f"{operation} without WHERE clause affects all rows",
                code=f"{operation.lower()}-without-where",
                risk_level=RiskLevel.HIGH,
                expression=expression,
            )

    def add_error(
        self,
        context: "SQLProcessingContext",
        message: str,
        code: str,
        risk_level: RiskLevel,
        expression: "Optional[exp.Expression]" = None,
    ) -> None:
        """Add a validation error to the context."""
        error = ValidationError(
            message=message, code=code, risk_level=risk_level, processor=self.__class__.__name__, expression=expression
        )
        context.validation_errors.append(error)

    @staticmethod
    def _categorize_statement(expression: "exp.Expression") -> StatementCategory:
        if isinstance(expression, (exp.Create, exp.Alter, exp.Drop, exp.TruncateTable, exp.Comment)):
            return StatementCategory.DDL
        if isinstance(expression, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
            return StatementCategory.DQL
        if isinstance(expression, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
            return StatementCategory.DML
        if isinstance(expression, (exp.Grant,)):
            return StatementCategory.DCL
        if isinstance(expression, (exp.Commit, exp.Rollback)):
            return StatementCategory.TCL
        return StatementCategory.DQL

    @staticmethod
    def _get_operation_type(expression: "exp.Expression") -> str:
        return expression.__class__.__name__.upper()

    @staticmethod
    def _has_where_clause(expression: "exp.Expression") -> bool:
        if isinstance(expression, (exp.Delete, exp.Update)):
            return expression.args.get("where") is not None
        return True
