from typing import Optional

from sqlglot import exp
from typing_extensions import Self

from sqlspec.exceptions import SQLBuilderError

__all__ = ("UpdateTableClauseMixin", )


class UpdateTableClauseMixin:
    """Mixin providing TABLE clause for UPDATE builders."""

    def table(self, table_name: str, alias: Optional[str] = None) -> Self:
        """Set the table to update.

        Args:
            table_name: The name of the table.
            alias: Optional alias for the table.

        Returns:
            The current builder instance for method chaining.
        """
        if self._expression is None:
            self._expression = exp.Update(this=None, expressions=[], joins=[])  # type: ignore[attr-defined]
        if not isinstance(self._expression, exp.Update):  # type: ignore[attr-defined]
            msg = "Cannot set table on a non-UPDATE expression."
            raise SQLBuilderError(msg)

        table_expr: exp.Expression = exp.to_table(table_name, alias=alias)
        self._expression.set("this", table_expr)  # type: ignore[attr-defined]
        setattr(self, "_table", table_name)
        return self
