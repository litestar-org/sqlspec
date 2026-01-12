"""Shared query modification utilities for SQL and builder classes.

This module provides pure functions for building SQL expressions that can be
used by both the immutable SQL class and the mutable builder classes. All
functions are designed to be mypyc-compatible with no dynamic dispatch.

The utilities are organized in layers:
    - Expression factories: Create comparison expressions (eq, lt, like, etc.)
    - Condition builders: Create parameterized WHERE conditions
    - Expression modifiers: Apply WHERE, LIMIT, OFFSET to expressions
    - CTE utilities: Safe CTE extraction and reattachment

Example:
    >>> from sqlspec.core.query_modifiers import (
    ...     expr_eq,
    ...     create_condition,
    ...     apply_where,
    ... )
    >>> condition = create_condition(
    ...     "status", "status_param", expr_eq
    ... )
    >>> modified = apply_where(select_expr, condition)
"""

from collections.abc import Callable
from typing import Any

from sqlglot import exp

from sqlspec.exceptions import SQLSpecError

__all__ = (
    "apply_limit",
    "apply_offset",
    "apply_or_where",
    "apply_select_only",
    "apply_where",
    "create_between_condition",
    "create_condition",
    "create_exists_condition",
    "create_in_condition",
    "create_not_exists_condition",
    "create_not_in_condition",
    "expr_eq",
    "expr_gt",
    "expr_gte",
    "expr_ilike",
    "expr_is_not_null",
    "expr_is_null",
    "expr_like",
    "expr_lt",
    "expr_lte",
    "expr_neq",
    "expr_not_like",
    "extract_column_name",
    "parse_column_for_condition",
    "safe_modify_with_cte",
)

# Type alias for condition factory functions
ConditionFactory = Callable[[exp.Expression, exp.Placeholder], exp.Expression]


# =============================================================================
# Expression Factories
# =============================================================================


def expr_eq(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create equality expression: column = :param."""
    return exp.EQ(this=col, expression=placeholder)


def expr_neq(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create not-equal expression: column != :param."""
    return exp.NEQ(this=col, expression=placeholder)


def expr_lt(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create less-than expression: column < :param."""
    return exp.LT(this=col, expression=placeholder)


def expr_lte(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create less-than-or-equal expression: column <= :param."""
    return exp.LTE(this=col, expression=placeholder)


def expr_gt(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create greater-than expression: column > :param."""
    return exp.GT(this=col, expression=placeholder)


def expr_gte(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create greater-than-or-equal expression: column >= :param."""
    return exp.GTE(this=col, expression=placeholder)


def expr_like(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create LIKE expression: column LIKE :param."""
    return exp.Like(this=col, expression=placeholder)


def expr_not_like(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create NOT LIKE expression: NOT (column LIKE :param)."""
    return exp.Not(this=exp.Like(this=col, expression=placeholder))


def expr_ilike(col: exp.Expression, placeholder: exp.Placeholder) -> exp.Expression:
    """Create case-insensitive LIKE expression: column ILIKE :param."""
    return exp.ILike(this=col, expression=placeholder)


def expr_is_null(col: exp.Expression, _placeholder: exp.Placeholder) -> exp.Expression:
    """Create IS NULL expression: column IS NULL.

    Note: placeholder is ignored but kept for consistent factory signature.
    """
    return exp.Is(this=col, expression=exp.null())


def expr_is_not_null(col: exp.Expression, _placeholder: exp.Placeholder) -> exp.Expression:
    """Create IS NOT NULL expression: column IS NOT NULL.

    Note: placeholder is ignored but kept for consistent factory signature.
    """
    return exp.Not(this=exp.Is(this=col, expression=exp.null()))


# =============================================================================
# Column Parsing
# =============================================================================


def parse_column_for_condition(column: str | exp.Column | exp.Expression) -> exp.Expression:
    """Parse column specification for use in conditions.

    Handles various input formats:
        - "column_name" -> exp.Column
        - "table.column" -> exp.Column with table
        - exp.Column -> returned as-is
        - Other exp.Expression -> returned as-is

    Args:
        column: Column specification

    Returns:
        SQLGlot column expression
    """
    if isinstance(column, exp.Expression):
        return column

    if isinstance(column, str):
        if "." in column:
            parts = column.split(".", 1)
            return exp.column(parts[1], table=parts[0])
        return exp.column(column)

    return exp.column(str(column))


def extract_column_name(column: str | exp.Column | exp.Expression) -> str:
    """Extract column name from column expression for parameter naming.

    Args:
        column: Column expression (string or SQLGlot Column)

    Returns:
        Column name as string for use as parameter name base
    """
    if isinstance(column, str):
        return column.split(".")[-1] if "." in column else column

    if isinstance(column, exp.Column):
        return column.name

    if isinstance(column, exp.Expression) and hasattr(column, "name") and column.name:
        return str(column.name)

    return "column"


# =============================================================================
# Condition Builders
# =============================================================================


def create_condition(
    column: str | exp.Column | exp.Expression, param_name: str, condition_factory: ConditionFactory
) -> exp.Expression:
    """Create parameterized condition expression.

    This is a pure function - parameter value binding happens in the caller.

    Args:
        column: Column name or expression
        param_name: Pre-generated unique parameter name
        condition_factory: Factory function for the condition type

    Returns:
        Condition expression with placeholder
    """
    col_expr = parse_column_for_condition(column)
    placeholder = exp.Placeholder(this=param_name)
    return condition_factory(col_expr, placeholder)


def create_in_condition(column: str | exp.Column | exp.Expression, param_names: list[str]) -> exp.Expression:
    """Create IN condition with multiple placeholders.

    Args:
        column: Column name or expression
        param_names: Pre-generated parameter names (one per value)

    Returns:
        IN expression with placeholders
    """
    col_expr = parse_column_for_condition(column)
    placeholders = [exp.Placeholder(this=name) for name in param_names]
    return exp.In(this=col_expr, expressions=placeholders)


def create_not_in_condition(column: str | exp.Column | exp.Expression, param_names: list[str]) -> exp.Expression:
    """Create NOT IN condition with multiple placeholders.

    Args:
        column: Column name or expression
        param_names: Pre-generated parameter names (one per value)

    Returns:
        NOT IN expression with placeholders
    """
    in_expr = create_in_condition(column, param_names)
    return exp.Not(this=in_expr)


def create_between_condition(
    column: str | exp.Column | exp.Expression, low_param: str, high_param: str
) -> exp.Expression:
    """Create BETWEEN condition.

    Args:
        column: Column name or expression
        low_param: Parameter name for low bound
        high_param: Parameter name for high bound

    Returns:
        BETWEEN expression with placeholders
    """
    col_expr = parse_column_for_condition(column)
    low_placeholder = exp.Placeholder(this=low_param)
    high_placeholder = exp.Placeholder(this=high_param)
    return exp.Between(this=col_expr, low=low_placeholder, high=high_placeholder)


def create_exists_condition(subquery: exp.Expression) -> exp.Expression:
    """Create EXISTS condition.

    Args:
        subquery: Subquery expression

    Returns:
        EXISTS expression
    """
    return exp.Exists(this=subquery)


def create_not_exists_condition(subquery: exp.Expression) -> exp.Expression:
    """Create NOT EXISTS condition.

    Args:
        subquery: Subquery expression

    Returns:
        NOT EXISTS expression
    """
    return exp.Not(this=exp.Exists(this=subquery))


# =============================================================================
# Expression Modifiers
# =============================================================================


def apply_where(expression: exp.Expression, condition: exp.Expression) -> exp.Expression:
    """Apply WHERE condition to an expression using AND.

    Works with SELECT, UPDATE, and DELETE expressions.

    Args:
        expression: Base expression to modify (will be copied)
        condition: WHERE condition to add

    Returns:
        Modified expression with WHERE condition

    Raises:
        SQLSpecError: If expression type doesn't support WHERE
    """
    if not isinstance(expression, (exp.Select, exp.Update, exp.Delete)):
        msg = f"Cannot apply WHERE to {type(expression).__name__}"
        raise SQLSpecError(msg)

    return expression.where(condition, copy=False)


def apply_or_where(expression: exp.Expression, condition: exp.Expression) -> exp.Expression:
    """Apply WHERE condition to an expression using OR.

    Combines the new condition with any existing WHERE clause using OR.

    Args:
        expression: Base expression with existing WHERE
        condition: New condition to add with OR

    Returns:
        Modified expression with OR condition

    Raises:
        SQLSpecError: If expression type doesn't support WHERE or has no existing WHERE
    """
    if not isinstance(expression, (exp.Select, exp.Update, exp.Delete)):
        msg = f"Cannot apply WHERE to {type(expression).__name__}"
        raise SQLSpecError(msg)

    existing_where = expression.args.get("where")
    if not existing_where or not isinstance(existing_where, exp.Where):
        msg = "Cannot use OR without existing WHERE clause"
        raise SQLSpecError(msg)

    combined = exp.Or(this=existing_where.this, expression=condition)
    existing_where.set("this", combined)
    return expression


def apply_limit(expression: exp.Expression, limit_value: int) -> exp.Expression:
    """Apply LIMIT clause to expression.

    Args:
        expression: Base expression (must be SELECT)
        limit_value: LIMIT value

    Returns:
        Modified expression with LIMIT

    Raises:
        SQLSpecError: If expression is not SELECT
    """
    if not isinstance(expression, exp.Select):
        msg = f"LIMIT only valid for SELECT, got {type(expression).__name__}"
        raise SQLSpecError(msg)

    return expression.limit(limit_value, copy=False)


def apply_offset(expression: exp.Expression, offset_value: int) -> exp.Expression:
    """Apply OFFSET clause to expression.

    Args:
        expression: Base expression (must be SELECT)
        offset_value: OFFSET value

    Returns:
        Modified expression with OFFSET

    Raises:
        SQLSpecError: If expression is not SELECT
    """
    if not isinstance(expression, exp.Select):
        msg = f"OFFSET only valid for SELECT, got {type(expression).__name__}"
        raise SQLSpecError(msg)

    return expression.offset(offset_value, copy=False)


def apply_select_only(expression: exp.Expression, columns: tuple[str | exp.Expression, ...]) -> exp.Expression:
    """Replace SELECT clause with only specified columns.

    Args:
        expression: Base expression (must be SELECT)
        columns: Column names or expressions to select

    Returns:
        Modified expression with new SELECT columns

    Raises:
        SQLSpecError: If expression is not SELECT
    """
    if not isinstance(expression, exp.Select):
        msg = f"select_only only valid for SELECT, got {type(expression).__name__}"
        raise SQLSpecError(msg)

    expression.set("expressions", [])

    for col in columns:
        col_expr = parse_column_for_condition(col) if isinstance(col, str) else col
        expression = expression.select(col_expr, copy=False)

    return expression


# =============================================================================
# CTE Utilities
# =============================================================================


def safe_modify_with_cte(
    expression: exp.Expression, modification_fn: Callable[[exp.Expression], exp.Expression]
) -> exp.Expression:
    """Safely apply a modification, preserving CTEs at top level.

    This ensures CTEs stay at the outermost level even when the modification
    would normally wrap them in a subquery. This fixes issue #301 where
    CTEs inside subqueries generate invalid SQL.

    Args:
        expression: Expression that may contain CTEs
        modification_fn: Function to apply to the expression

    Returns:
        Modified expression with CTE preserved at top level
    """
    cte: Any = None
    working_expr = expression

    if isinstance(expression, exp.Select):
        cte = expression.args.get("with_")
        if cte:
            working_expr = expression.copy()
            working_expr.set("with_", None)

    result = modification_fn(working_expr)

    if cte and isinstance(result, exp.Select):
        result.set("with_", cte)

    return result
