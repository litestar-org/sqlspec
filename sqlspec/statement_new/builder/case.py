"""CASE expression builder."""
from typing import TYPE_CHECKING, Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

if TYPE_CHECKING:
    from sqlspec.protocols import SQLBuilderProtocol


class CaseBuilder:
    """Builder for CASE expressions."""

    def __init__(self, parent: "SQLBuilderProtocol", expression: Optional[Union[str, exp.Expression]] = None) -> None:
        """Initialize CASE builder.

        Args:
            parent: The parent builder that created this CASE expression.
            expression: Optional expression for a searched CASE statement.
                        If None, creates a simple CASE statement.
        """
        self._parent = parent
        self._case_expr = exp.Case()

        if expression is not None:
            if isinstance(expression, str):
                parsed = exp.maybe_parse(expression, dialect=getattr(parent, "dialect", None))
                if not parsed:
                    msg = f"Could not parse CASE expression: {expression}"
                    raise SQLBuilderError(msg)
                self._case_expr.set("this", parsed)
            else:
                self._case_expr.set("this", expression)

        self._when_conditions: list[exp.When] = []
        self._else_expr: Optional[exp.Expression] = None

    def when(self, condition: Union[str, exp.Expression], then: Any) -> Self:
        """Add a WHEN clause to the CASE expression.

        Args:
            condition: The condition to check. For simple CASE, this is the value to compare.
                       For searched CASE, this is a boolean expression.
            then: The value to return when the condition is true.

        Returns:
            The current CaseBuilder instance for method chaining.
        """
        # Parse condition
        if isinstance(condition, str):
            condition_expr = exp.maybe_parse(condition, dialect=getattr(self._parent, "dialect", None))
            if not condition_expr:
                msg = f"Could not parse WHEN condition: {condition}"
                raise SQLBuilderError(msg)
        else:
            condition_expr = condition

        # Parameterize the then value
        if hasattr(self._parent, "add_parameter"):
            _, param_name = self._parent.add_parameter(then)
            then_expr = exp.var(param_name)
        else:
            then_expr = exp.Literal.string(str(then)) if isinstance(then, str) else exp.Literal.number(then)

        # Create WHEN clause
        when_clause = exp.When(this=condition_expr, then=then_expr)
        self._when_conditions.append(when_clause)

        return self

    def else_(self, value: Any) -> Self:
        """Add an ELSE clause to the CASE expression.

        Args:
            value: The default value to return when no WHEN conditions match.

        Returns:
            The current CaseBuilder instance for method chaining.
        """
        # Parameterize the else value
        if hasattr(self._parent, "add_parameter"):
            _, param_name = self._parent.add_parameter(value)
            self._else_expr = exp.var(param_name)
        else:
            self._else_expr = exp.Literal.string(str(value)) if isinstance(value, str) else exp.Literal.number(value)

        return self

    def end(self, alias: Optional[str] = None) -> "SQLBuilderProtocol":
        """Complete the CASE expression and return to the parent builder.

        Args:
            alias: Optional alias for the CASE expression.

        Returns:
            The parent builder with the CASE expression added.
        """
        if not self._when_conditions:
            msg = "CASE expression must have at least one WHEN clause"
            raise SQLBuilderError(msg)

        # Set the WHEN clauses
        self._case_expr.set("ifs", self._when_conditions)

        # Set the ELSE clause if provided
        if self._else_expr:
            self._case_expr.set("default", self._else_expr)

        # Add alias if provided
        case_expr = exp.alias_(self._case_expr, alias) if alias else self._case_expr

        # Add to parent's SELECT clause if it's a SELECT statement
        if hasattr(self._parent, "_expression") and isinstance(self._parent._expression, exp.Select):
            self._parent._expression = self._parent._expression.select(case_expr, copy=False)
        elif hasattr(self._parent, "select"):
            # Use the select method if available
            self._parent.select(case_expr)
        else:
            msg = "CASE expressions can only be added to SELECT statements"
            raise SQLBuilderError(msg)

        return self._parent
