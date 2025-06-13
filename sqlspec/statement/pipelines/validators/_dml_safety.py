# DML Safety Validator - Consolidates risky DML operations and DDL prevention
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

import sqlglot.expressions as exp

from sqlspec.exceptions import RiskLevel
from sqlspec.statement.pipelines.results import ProcessorResult
from sqlspec.statement.pipelines.validators.base import BaseValidator

if TYPE_CHECKING:
    from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("DMLSafetyConfig", "DMLSafetyValidator", "SafetyIssue", "StatementCategory")


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


@dataclass
class SafetyIssue:
    """Represents a safety issue found during validation."""

    category: StatementCategory
    operation: str
    risk_level: RiskLevel
    description: str
    recommendation: str
    affected_tables: "list[str]" = field(default_factory=list)
    estimated_impact: "Optional[str]" = None


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

    def _process_internal(self, context: "SQLProcessingContext") -> ProcessorResult:
        """Process SQL statement for safety validation.

        Args:
            context: The SQL processing context

        Returns:
            ProcessorResult with validation findings
        """
        issues: list[SafetyIssue] = []
        risk_level = RiskLevel.SKIP

        if context.current_expression is None:
            return self._create_result(
                expression=None,
                is_safe=True,
                risk_level=RiskLevel.SKIP,
                issues=["No expression to validate"],
                metadata={"skipped": True},
            )

        # Categorize statement
        category = self._categorize_statement(context.current_expression)
        operation = self._get_operation_type(context.current_expression)

        # Check DDL restrictions
        if category == StatementCategory.DDL and self.config.prevent_ddl:
            if operation not in self.config.allowed_ddl_operations:
                issues.append(
                    SafetyIssue(
                        category=category,
                        operation=operation,
                        risk_level=RiskLevel.CRITICAL,
                        description=f"DDL operation '{operation}' is not allowed",
                        recommendation="Use migration tools for schema changes",
                    )
                )

        # Check DML safety
        elif category == StatementCategory.DML:
            if operation in self.config.require_where_clause and not self._has_where_clause(context.current_expression):
                issues.append(
                    SafetyIssue(
                        category=category,
                        operation=operation,
                        risk_level=RiskLevel.HIGH,
                        description=f"{operation} without WHERE clause affects all rows",
                        recommendation="Add WHERE clause or use TRUNCATE if intentional",
                    )
                )

            # Check affected row limits
            if self.config.max_affected_rows:
                estimated_rows = self._estimate_affected_rows(context.current_expression)
                if estimated_rows > self.config.max_affected_rows:
                    issues.append(
                        SafetyIssue(
                            category=category,
                            operation=operation,
                            risk_level=RiskLevel.MEDIUM,
                            description=f"Operation may affect {estimated_rows:,} rows (limit: {self.config.max_affected_rows:,})",
                            recommendation="Consider batching or increasing limit",
                        )
                    )

        # Check DCL restrictions
        elif category == StatementCategory.DCL and self.config.prevent_dcl:
            issues.append(
                SafetyIssue(
                    category=category,
                    operation=operation,
                    risk_level=RiskLevel.HIGH,
                    description=f"DCL operation '{operation}' is not allowed",
                    recommendation="Contact DBA for permission changes",
                )
            )

        # Determine overall risk
        if issues:
            risk_level = max(issue.risk_level for issue in issues)

        # Build metadata
        metadata = {
            "statement_category": category.value,
            "operation": operation,
            "safety_issues": [self._issue_to_dict(issue) for issue in issues],
            "has_where_clause": self._has_where_clause(context.current_expression),
            "affected_tables": self._extract_affected_tables(context.current_expression),
            "migration_mode": self.config.migration_mode,
        }

        # Return result
        return self._create_result(
            expression=context.current_expression,
            is_safe=len(issues) == 0,
            risk_level=risk_level,
            issues=[issue.description for issue in issues],
            warnings=[issue.recommendation for issue in issues if issue.risk_level == RiskLevel.LOW],
            metadata=metadata,
        )

    def _categorize_statement(self, expression: "exp.Expression") -> StatementCategory:
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

    def _get_operation_type(self, expression: "exp.Expression") -> str:
        """Get specific operation name.

        Args:
            expression: The SQL expression

        Returns:
            The operation type as string
        """
        return expression.__class__.__name__.upper()

    def _has_where_clause(self, expression: "exp.Expression") -> bool:
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

    def _has_unique_condition(self, where: "exp.Expression") -> bool:
        """Check if WHERE clause uses unique columns.

        Args:
            where: The WHERE expression

        Returns:
            True if unique condition found
        """
        # Look for id = value patterns
        for condition in where.find_all(exp.EQ):
            if isinstance(condition.left, exp.Column):
                col_name = condition.left.name.lower()
                if col_name in {"id", "uuid", "guid", "pk", "primary_key"}:
                    return True
        return False

    @staticmethod
    def _has_indexed_condition(where: "exp.Expression") -> bool:
        """Check if WHERE clause uses indexed columns.

        Args:
            where: The WHERE expression

        Returns:
            True if indexed condition found
        """
        # Look for common indexed column patterns
        for condition in where.find_all(exp.Predicate):
            if hasattr(condition, "left") and isinstance(condition.left, exp.Column):
                col_name = condition.left.name.lower()
                # Common indexed columns
                if col_name in {"created_at", "updated_at", "email", "username", "status", "type"}:
                    return True
        return False

    def _extract_affected_tables(self, expression: "exp.Expression") -> "list[str]":
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
            and isinstance(expression.this, (exp.Table, exp.Identifier))
        ):
            tables.append(expression.this.name)

        return tables

    def _issue_to_dict(self, issue: SafetyIssue) -> "dict[str, Any]":
        """Convert SafetyIssue to dictionary.

        Args:
            issue: The safety issue

        Returns:
            Dictionary representation
        """
        return {
            "category": issue.category.value,
            "operation": issue.operation,
            "risk_level": issue.risk_level.name,
            "description": issue.description,
            "recommendation": issue.recommendation,
            "affected_tables": issue.affected_tables,
            "estimated_impact": issue.estimated_impact,
        }
