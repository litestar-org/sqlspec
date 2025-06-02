"""Replaces literals in SQL with placeholders and extracts them."""

from typing import Any, Optional

from sqlglot import exp

from sqlspec.statement.pipelines.base import ProcessorProtocol, ValidationResult
from sqlspec.statement.pipelines.context import SQLProcessingContext

__all__ = ("ParameterizeLiterals",)

# Constants for literal parameterization
DEFAULT_MAX_STRING_LENGTH = 1000
"""Default maximum string length for literal parameterization."""


class ParameterizeLiterals(ProcessorProtocol[exp.Expression]):
    """Replaces literals in SQL queries with parameter placeholders (e.g., ?, :name).

    The extracted literals can then be passed as separate arguments to the database driver,
    improving security (preventing SQL injection) and potentially performance
    (query plan caching).

    Args:
        placeholder_style: Style of placeholder to use ("?", ":name", "$1", etc.).
        preserve_null: Whether to preserve NULL literals as-is.
        preserve_boolean: Whether to preserve boolean literals as-is.
        preserve_numbers_in_limit: Whether to preserve numbers in LIMIT/OFFSET clauses.
        max_string_length: Maximum string length to parameterize (longer strings left as-is).
    """

    def __init__(
        self,
        placeholder_style: str = "?",
        preserve_null: bool = True,
        preserve_boolean: bool = True,
        preserve_numbers_in_limit: bool = True,
        max_string_length: int = DEFAULT_MAX_STRING_LENGTH,
    ) -> None:
        self.placeholder_style = placeholder_style
        self.preserve_null = preserve_null
        self.preserve_boolean = preserve_boolean
        self.preserve_numbers_in_limit = preserve_numbers_in_limit
        self.max_string_length = max_string_length
        self.extracted_parameters: list[Any] = []  # Instance variable to collect params for current run
        self._parameter_counter = 0  # Instance variable for naming placeholders

    def process(self, context: SQLProcessingContext) -> tuple[exp.Expression, Optional[ValidationResult]]:
        """Replaces literals with placeholders if not already parameterized (based on context.config.input_sql_had_placeholders)."""
        assert context.current_expression is not None, (
            "ParameterizeLiterals expects a valid current_expression in context."
        )

        self.extracted_parameters = []
        self._parameter_counter = 0

        if context.config.input_sql_had_placeholders:
            return context.current_expression, None

        # Proceed with parameterizing literals
        def _parameterize_literal_node(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Literal):
                return self._process_literal(node)
            return node

        transformed_expression = context.current_expression.transform(_parameterize_literal_node, copy=True)
        context.current_expression = transformed_expression

        if (
            not hasattr(context, "extracted_parameters_from_pipeline")
            or context.extracted_parameters_from_pipeline is None
        ):
            context.extracted_parameters_from_pipeline = []
        context.extracted_parameters_from_pipeline.extend(self.extracted_parameters)

        return context.current_expression, None

    def _process_literal(self, literal: exp.Literal) -> exp.Expression:
        """Process a single literal and decide whether to parameterize it."""
        # Check if this literal should be preserved
        if self._should_preserve_literal(literal):
            return literal

        # Extract the literal value
        literal_value = self._extract_literal_value(literal)

        # Add to parameters list
        self.extracted_parameters.append(literal_value)

        # Create appropriate placeholder
        return self._create_placeholder()

    def _should_preserve_literal(self, literal: exp.Literal) -> bool:
        """Determine if a literal should be preserved (not parameterized)."""
        # Check for NULL values
        if self.preserve_null and (literal.this is None or str(literal.this).upper() == "NULL"):
            return True

        # Check for boolean values
        if self.preserve_boolean and isinstance(literal.this, bool):
            return True

        # Check if literal is in a context where parameterization might not be suitable
        parent = literal.parent
        while parent:
            # Preserve numbers in LIMIT/OFFSET clauses
            if (
                self.preserve_numbers_in_limit
                and isinstance(parent, (exp.Limit, exp.Offset))
                and self._is_number_literal(literal)
            ):
                return True
            if isinstance(parent, (exp.DataType, exp.ColumnDef, exp.Create, exp.Schema)):
                return True

            parent = parent.parent

        # Check string length
        if self._is_string_literal(literal):
            string_value = str(literal.this)
            if len(string_value) > self.max_string_length:
                return True

        return False

    def _extract_literal_value(self, literal: exp.Literal) -> Any:
        """Extract the Python value from a SQLGlot literal."""
        if literal.this is None or str(literal.this).upper() == "NULL":
            return None
        if self._is_string_literal(literal):
            return str(literal.this)
        if self._is_number_literal(literal):
            # Try to preserve the original numeric type
            try:
                if "." in str(literal.this) or "e" in str(literal.this).lower():
                    return float(literal.this)
                return int(literal.this)
            except (ValueError, TypeError):
                return str(literal.this)
        elif isinstance(literal.this, bool):
            return literal.this
        else:
            # Fallback to string representation
            return str(literal.this)

    def _is_string_literal(self, literal: exp.Literal) -> bool:
        """Check if a literal is a string."""
        # Check if it's explicitly a string literal
        return (hasattr(literal, "is_string") and literal.is_string) or (
            isinstance(literal.this, str) and not self._is_number_literal(literal)
        )

    @staticmethod
    def _is_number_literal(literal: exp.Literal) -> bool:
        """Check if a literal is a number."""
        # Check if it's explicitly a number literal
        if hasattr(literal, "is_number") and literal.is_number:
            return True
        if literal.this is None:
            return False
        # Try to determine if it's numeric by attempting conversion
        try:
            float(str(literal.this))
        except (ValueError, TypeError):
            return False
        return True

    def _create_placeholder(self) -> exp.Expression:
        """Create a placeholder expression based on the configured style."""
        if self.placeholder_style == "?":
            return exp.Placeholder()
        if self.placeholder_style.startswith(":"):
            self._parameter_counter += 1
            if self.placeholder_style == ":name":
                param_name = f"param_{self._parameter_counter}"
            else:
                param_name = f"param_{self._parameter_counter}"
            return exp.Placeholder(this=param_name)
        if self.placeholder_style.startswith("$"):
            # PostgreSQL style numbered parameters
            self._parameter_counter += 1
            return exp.Placeholder(this=f"${self._parameter_counter}")
        # Default to question mark
        return exp.Placeholder()

    def get_parameters(self) -> list[Any]:
        """Get the list of extracted parameters from the last processing operation.

        Returns:
            List of parameter values extracted during the last process() call.
        """
        return self.extracted_parameters.copy()

    def clear_parameters(self) -> None:
        """Clear the extracted parameters list."""
        self.extracted_parameters = []
        self._parameter_counter = 0
