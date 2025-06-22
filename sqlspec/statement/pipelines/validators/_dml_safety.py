# DML Safety Validator - Consolidates risky DML operations and DDL prevention
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Optional

from sqlglot import expressions as exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.validators.base import BaseValidator

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("DMLSafetyConfig", "DMLSafetyValidator", "StatementCategory")


class StatementCategory(Enum):
    """Categories for SQL statement types."""

    DDL = "ddl"  # CREATE, ALTER, DROP, TRUNCATE
    DML = "dml"  # INSERT, UPDATE, DELETE, MERGE
    DQL = "dql"  # SELECT
    DCL = "dcl"  # GRANT, REVOKE
    TCL = "tcl"  # COMMIT, ROLLBACK, SAVEPOINT


@dataclass
class DMLSafetyConfig:
    """Configuration for DML safety validation."""

    prevent_ddl: bool = True
    prevent_dcl: bool = True
    require_where_clause: "set[str]" = field(default_factory=lambda: {"DELETE", "UPDATE"})
    allowed_ddl_operations: "set[str]" = field(default_factory=set)
    migration_mode: bool = False  # Allow DDL in migration contexts
    max_affected_rows: "Optional[int]" = None  # Limit for DML operations


class DMLSafetyValidator(BaseValidator):
    """Unified validator for DML/DDL safety checks.

    This validator consolidates:
    - DDL prevention (CREATE, ALTER, DROP, etc.)
    - Risky DML detection (DELETE/UPDATE without WHERE)
    - DCL restrictions (GRANT, REVOKE)
    - Row limit enforcement
    """

    def __init__(self, config: "Optional[DMLSafetyConfig]" = None) -> None:
        """Initialize the DML safety validator.

        Args:
            config: Configuration for safety validation
        """
        super().__init__()
        self.config = config or DMLSafetyConfig()

    def validate(self, expression: "exp.Expression", context: "SQLProcessingContext") -> None:
        """Validate SQL statement for safety issues.

        Args:
            expression: The SQL expression to validate
            context: The SQL processing context
        """
        # Categorize statement
        category = self._categorize_statement(expression)
        operation = self._get_operation_type(expression)

        # Check DDL restrictions
        if category == StatementCategory.DDL and self.config.prevent_ddl:
            if operation not in self.config.allowed_ddl_operations:
                self.add_error(
                    context,
                    message=f"DDL operation '{operation}' is not allowed",
                    code="ddl-not-allowed",
                    risk_level=RiskLevel.CRITICAL,
                    expression=expression,
                )

        # Check DML safety
        elif category == StatementCategory.DML:
            if operation in self.config.require_where_clause and not self._has_where_clause(expression):
                self.add_error(
                    context,
                    message=f"{operation} without WHERE clause affects all rows",
                    code=f"{operation.lower()}-without-where",
                    risk_level=RiskLevel.HIGH,
                    expression=expression,
                )

            # Check affected row limits
            if self.config.max_affected_rows:
                estimated_rows = self._estimate_affected_rows(expression)
                if estimated_rows > self.config.max_affected_rows:
                    self.add_error(
                        context,
                        message=f"Operation may affect {estimated_rows:,} rows (limit: {self.config.max_affected_rows:,})",
                        code="excessive-rows-affected",
                        risk_level=RiskLevel.MEDIUM,
                        expression=expression,
                    )

        # Check DCL restrictions
        elif category == StatementCategory.DCL and self.config.prevent_dcl:
            self.add_error(
                context,
                message=f"DCL operation '{operation}' is not allowed",
                code="dcl-not-allowed",
                risk_level=RiskLevel.HIGH,
                expression=expression,
            )

        # Store metadata in context
        context.metadata[self.__class__.__name__] = {
            "statement_category": category.value,
            "operation": operation,
            "has_where_clause": self._has_where_clause(expression) if category == StatementCategory.DML else None,
            "affected_tables": self._extract_affected_tables(expression),
            "migration_mode": self.config.migration_mode,
        }

    @staticmethod
    def _categorize_statement(expression: "exp.Expression") -> StatementCategory:
        """Categorize SQL statement type.

        Args:
            expression: The SQL expression to categorize

        Returns:
            The statement category
        """
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

        return StatementCategory.DQL  # Default to query

    @staticmethod
    def _get_operation_type(expression: "exp.Expression") -> str:
        """Get specific operation name.

        Args:
            expression: The SQL expression

        Returns:
            The operation type as string
        """
        return expression.__class__.__name__.upper()

    @staticmethod
    def _has_where_clause(expression: "exp.Expression") -> bool:
        """Check if DML statement has WHERE clause.

        Args:
            expression: The SQL expression to check

        Returns:
            True if WHERE clause exists, False otherwise
        """
        if isinstance(expression, (exp.Delete, exp.Update)):
            return expression.args.get("where") is not None
        return True  # Other statements don't require WHERE

    def _estimate_affected_rows(self, expression: "exp.Expression") -> int:
        """Estimate number of rows affected by DML operation.

        Args:
            expression: The SQL expression

        Returns:
            Estimated number of affected rows
        """
        # Simple heuristic - can be enhanced with table statistics
        if not self._has_where_clause(expression):
            return 999999999  # Large number to indicate all rows

        where = expression.args.get("where")
        if where:
            # Check for primary key or unique conditions
            if self._has_unique_condition(where):
                return 1
            # Check for indexed conditions
            if self._has_indexed_condition(where):
                return 100  # Rough estimate

        return 10000  # Conservative estimate

    @staticmethod
    def _has_unique_condition(where: "Optional[exp.Expression]") -> bool:
        """Check if WHERE clause uses unique columns.

        Args:
            where: The WHERE expression

        Returns:
            True if unique condition found
        """
        if where is None:
            return False
        # Look for id = value patterns
        for condition in where.find_all(exp.EQ):
            if isinstance(condition.left, exp.Column):
                col_name = condition.left.name.lower()
                if col_name in {"id", "uuid", "guid", "pk", "primary_key"}:
                    return True
        return False

    @staticmethod
    def _has_indexed_condition(where: "Optional[exp.Expression]") -> bool:
        """Check if WHERE clause uses indexed columns.

        Args:
            where: The WHERE expression

        Returns:
            True if indexed condition found
        """
        if where is None:
            return False
        # Look for common indexed column patterns
        for condition in where.find_all(exp.Predicate):
            if hasattr(condition, "left") and isinstance(condition.left, exp.Column):  # pyright: ignore
                col_name = condition.left.name.lower()  # pyright: ignore
                # Common indexed columns
                if col_name in {"created_at", "updated_at", "email", "username", "status", "type"}:
                    return True
        return False

    @staticmethod
    def _extract_affected_tables(expression: "exp.Expression") -> "list[str]":
        """Extract table names affected by the statement.

        Args:
            expression: The SQL expression

        Returns:
            List of affected table names
        """
        tables = []

        # For DML statements
        if isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            if hasattr(expression, "this") and expression.this:
                table_expr = expression.this
                if isinstance(table_expr, exp.Table):
                    tables.append(table_expr.name)

        # For DDL statements
        elif (
            isinstance(expression, (exp.Create, exp.Drop, exp.Alter))
            and hasattr(expression, "this")
            and expression.this
        ):
            # For CREATE TABLE, the table is in expression.this.this
            if isinstance(expression, exp.Create) and isinstance(expression.this, exp.Schema):
                if hasattr(expression.this, "this") and expression.this.this:
                    table_expr = expression.this.this
                    if isinstance(table_expr, exp.Table):
                        tables.append(table_expr.name)
            # For DROP/ALTER, table is directly in expression.this
            elif isinstance(expression.this, (exp.Table, exp.Identifier)):
                tables.append(expression.this.name)

        return tables
