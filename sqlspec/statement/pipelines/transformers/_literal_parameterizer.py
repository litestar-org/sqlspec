"""Replaces literals in SQL with placeholders and extracts them."""

from typing import TYPE_CHECKING, Any, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.statement.sql import SQLConfig


__all__ = ("ParameterizeLiterals",)


class ParameterizeLiterals(ProcessorProtocol[exp.Expression]):
    """Replaces literals in SQL queries with parameter placeholders (e.g., ?, :name).

    The extracted literals can then be passed as separate arguments to the database driver,
    improving security (preventing SQL injection) and potentially performance
    (query plan caching).
    """

    def __init__(self, placeholder_style: str = "?") -> None:
        self.placeholder_style = placeholder_style
        self.extracted_parameters: list[Any] = []

    def process(
        self,
        expression: exp.Expression,
        dialect: "Optional[DialectType]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "tuple[exp.Expression, Optional[ValidationResult]]":
        """Replaces literals with placeholders in the given SQL expression.

        Args:
            expression: The SQL expression to process.
            dialect: The SQL dialect.
            config: SQL configuration.

        Returns:
            A tuple containing the modified expression with literals parameterized
            and None for ValidationResult. The extracted parameters are stored in
            `self.extracted_parameters`.
        """
        self.extracted_parameters = []
        # Placeholder: Actual literal parameterization logic.
        # This is a complex transformation.
        # sqlglot.optimizer.normalize.normalize might offer some parts of this.
        # A simple approach might be to find all exp.Literal, replace them with a placeholder,
        # and add their values to self.extracted_parameters.
        #
        # Example sketch:
        # count = 0
        # def _parameterize(node):
        #    nonlocal count
        #    if isinstance(node, exp.Literal) and not isinstance(node.parent, exp.DataType):
        #        self.extracted_parameters.append(node.this)
        #        count += 1
        #        if self.placeholder_style == "?":
        #            return exp.Placeholder()
        #        elif self.placeholder_style == ":name": # or other named styles
        #            return exp.Placeholder(this=f"param_{count}")
        #    return node
        # parameterized_expression = expression.transform(_parameterize, copy=True)
        # return parameterized_expression, None

        return expression, None
