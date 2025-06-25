from typing import Any, Optional, Union

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("WindowFunctionsMixin",)


class WindowFunctionsMixin:
    """Mixin providing window function methods for SQL builders."""

    _expression: Optional[exp.Expression] = None

    def window(
        self,
        function_expr: Union[str, exp.Expression],
        partition_by: Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]] = None,
        order_by: Optional[Union[str, list[str], exp.Expression, list[exp.Expression]]] = None,
        frame: Optional[str] = None,
        alias: Optional[str] = None,
    ) -> Self:
        """Add a window function to the SELECT clause.

        Args:
            function_expr: The window function expression (e.g., "COUNT(*)", "ROW_NUMBER()").
            partition_by: Column(s) to partition by.
            order_by: Column(s) to order by within the window.
            frame: Window frame specification (e.g., "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW").
            alias: Optional alias for the window function.

        Raises:
            SQLBuilderError: If the current expression is not a SELECT statement or function parsing fails.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Select()
        if not isinstance(self._expression, exp.Select):
            msg = "Cannot add window function to a non-SELECT expression."
            raise SQLBuilderError(msg)

        func_expr_parsed: exp.Expression
        if isinstance(function_expr, str):
            parsed: Optional[exp.Expression] = exp.maybe_parse(function_expr, dialect=getattr(self, "dialect", None))
            if not parsed:
                msg = f"Could not parse function expression: {function_expr}"
                raise SQLBuilderError(msg)
            func_expr_parsed = parsed
        else:
            func_expr_parsed = function_expr

        over_args: dict[str, Any] = {}  # Stringified dict
        if partition_by:
            if isinstance(partition_by, str):
                over_args["partition_by"] = [exp.column(partition_by)]
            elif isinstance(partition_by, list):  # Check for list
                over_args["partition_by"] = [exp.column(col) if isinstance(col, str) else col for col in partition_by]
            elif isinstance(partition_by, exp.Expression):  # Check for exp.Expression
                over_args["partition_by"] = [partition_by]

        if order_by:
            if isinstance(order_by, str):
                over_args["order"] = exp.column(order_by).asc()
            elif isinstance(order_by, list):
                # Properly handle multiple ORDER BY columns using Order expression
                order_expressions: list[Union[exp.Expression, exp.Column]] = []
                for col in order_by:
                    if isinstance(col, str):
                        order_expressions.append(exp.column(col).asc())
                    else:
                        order_expressions.append(col)
                over_args["order"] = exp.Order(expressions=order_expressions)
            elif isinstance(order_by, exp.Expression):
                over_args["order"] = order_by

        if frame:
            frame_expr: Optional[exp.Expression] = exp.maybe_parse(frame, dialect=getattr(self, "dialect", None))
            if frame_expr:
                over_args["frame"] = frame_expr

        window_expr = exp.Window(this=func_expr_parsed, **over_args)
        self._expression.select(exp.alias_(window_expr, alias) if alias else window_expr, copy=False)
        return self
