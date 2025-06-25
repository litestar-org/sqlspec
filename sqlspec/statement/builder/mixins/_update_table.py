from typing import Optional

from sqlglot import exp
from typing_extensions import Self

__all__ = ("UpdateTableClauseMixin",)


class UpdateTableClauseMixin:
    """Mixin providing TABLE clause for UPDATE builders."""

    _expression: Optional[exp.Expression] = None

    def table(self, table_name: str, alias: Optional[str] = None) -> Self:
        """Set the table to update.

        Args:
            table_name: The name of the table.
            alias: Optional alias for the table.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None or not isinstance(self._expression, exp.Update):
            self._expression = exp.Update(this=None, expressions=[], joins=[])
        table_expr: exp.Expression = exp.to_table(table_name, alias=alias)
        self._expression.set("this", table_expr)
        setattr(self, "_table", table_name)
        return self
