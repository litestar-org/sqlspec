"""Replaces literals in SQL with placeholders and extracts them using SQLGlot AST."""

from dataclasses import dataclass
from typing import Any, Optional

from sqlglot import exp
from sqlglot.expressions import (
    Array,
    Binary,
    Boolean,
    DataType,
    Func,
    Literal,
    Null,
)

from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.context import SQLProcessingContext
from sqlspec.statement.pipelines.results import ProcessorResult

__all__ = ("ParameterizationContext", "ParameterizeLiterals")

# Constants for magic values and literal parameterization
MAX_DECIMAL_PRECISION = 6
MAX_INT32_VALUE = 2147483647
DEFAULT_MAX_STRING_LENGTH = 1000
"""Default maximum string length for literal parameterization."""

DEFAULT_MAX_ARRAY_LENGTH = 100
"""Default maximum array length for literal parameterization."""

DEFAULT_MAX_IN_LIST_SIZE = 50
"""Default maximum IN clause list size before parameterization."""


@dataclass
class ParameterizationContext:
    """Context for tracking parameterization state during AST traversal."""

    parent_stack: list[exp.Expression]
    in_function_args: bool = False
    in_case_when: bool = False
    in_array: bool = False
    in_in_clause: bool = False
    function_depth: int = 0


class ParameterizeLiterals(ProcessorProtocol[exp.Expression]):
    """Advanced literal parameterization using SQLGlot AST analysis.

    This enhanced version provides:
    - Context-aware parameterization based on AST position
    - Smart handling of arrays, IN clauses, and function arguments
    - Type-preserving parameter extraction
    - Configurable parameterization strategies
    - Performance optimization for query plan caching

    Args:
        placeholder_style: Style of placeholder to use ("?", ":name", "$1", etc.).
        preserve_null: Whether to preserve NULL literals as-is.
        preserve_boolean: Whether to preserve boolean literals as-is.
        preserve_numbers_in_limit: Whether to preserve numbers in LIMIT/OFFSET clauses.
        preserve_in_functions: List of function names where literals should be preserved.
        parameterize_arrays: Whether to parameterize array literals.
        parameterize_in_lists: Whether to parameterize IN clause lists.
        max_string_length: Maximum string length to parameterize.
        max_array_length: Maximum array length to parameterize.
        max_in_list_size: Maximum IN list size to parameterize.
        type_preservation: Whether to preserve exact literal types.
    """

    def __init__(
        self,
        placeholder_style: str = "?",
        preserve_null: bool = True,
        preserve_boolean: bool = True,
        preserve_numbers_in_limit: bool = True,
        preserve_in_functions: Optional[list[str]] = None,
        parameterize_arrays: bool = True,
        parameterize_in_lists: bool = True,
        max_string_length: int = DEFAULT_MAX_STRING_LENGTH,
        max_array_length: int = DEFAULT_MAX_ARRAY_LENGTH,
        max_in_list_size: int = DEFAULT_MAX_IN_LIST_SIZE,
        type_preservation: bool = True,
    ) -> None:
        self.placeholder_style = placeholder_style
        self.preserve_null = preserve_null
        self.preserve_boolean = preserve_boolean
        self.preserve_numbers_in_limit = preserve_numbers_in_limit
        self.preserve_in_functions = preserve_in_functions or ["COALESCE", "IFNULL", "NVL", "ISNULL"]
        self.parameterize_arrays = parameterize_arrays
        self.parameterize_in_lists = parameterize_in_lists
        self.max_string_length = max_string_length
        self.max_array_length = max_array_length
        self.max_in_list_size = max_in_list_size
        self.type_preservation = type_preservation
        self.extracted_parameters: list[Any] = []
        self._parameter_counter = 0
        self._parameter_metadata: list[dict[str, Any]] = []  # Track parameter types and context

    def process(self, context: SQLProcessingContext) -> "ProcessorResult":
        """Advanced literal parameterization with context-aware AST analysis."""
        if context.current_expression is None or context.config.input_sql_had_placeholders:
            return ProcessorResult(expression=context.current_expression)

        self.extracted_parameters = []
        self._parameter_counter = 0
        self._parameter_metadata = []

        param_context = ParameterizationContext(parent_stack=[])
        transformed_expression = self._transform_with_context(context.current_expression.copy(), param_context)
        context.current_expression = transformed_expression

        if context.extracted_parameters_from_pipeline is None:
            context.extracted_parameters_from_pipeline = []
        context.extracted_parameters_from_pipeline.extend(self.extracted_parameters)

        context.set_additional_data("parameter_metadata", self._parameter_metadata)

        metadata = {
            "type": "transformer",
            "parameters_extracted": len(self.extracted_parameters),
        }
        return ProcessorResult(expression=context.current_expression, metadata=metadata)

    def _transform_with_context(self, node: exp.Expression, context: ParameterizationContext) -> exp.Expression:
        """Transform expression tree with context tracking."""
        # Update context based on node type
        self._update_context(node, context, entering=True)

        # Process the node
        if isinstance(node, Literal):
            result = self._process_literal_with_context(node, context)
        elif isinstance(node, Array) and self.parameterize_arrays:
            result = self._process_array(node, context)
        elif isinstance(node, exp.In) and self.parameterize_in_lists:
            result = self._process_in_clause(node, context)
        else:
            # Recursively process children
            for key, value in node.args.items():
                if isinstance(value, exp.Expression):
                    node.set(key, self._transform_with_context(value, context))
                elif isinstance(value, list):
                    node.set(
                        key,
                        [
                            self._transform_with_context(v, context) if isinstance(v, exp.Expression) else v
                            for v in value
                        ],
                    )
            result = node

        # Update context when leaving
        self._update_context(node, context, entering=False)

        return result

    def _update_context(self, node: exp.Expression, context: ParameterizationContext, entering: bool) -> None:
        """Update parameterization context based on current AST node."""
        if entering:
            context.parent_stack.append(node)

            if isinstance(node, Func):
                context.function_depth += 1
                # Get function name from class name or node.name
                func_name = node.__class__.__name__.upper()
                if func_name in self.preserve_in_functions or (
                    node.name and node.name.upper() in self.preserve_in_functions
                ):
                    context.in_function_args = True
            elif isinstance(node, exp.Case):
                context.in_case_when = True
            elif isinstance(node, Array):
                context.in_array = True
            elif isinstance(node, exp.In):
                context.in_in_clause = True
        else:
            if context.parent_stack:
                context.parent_stack.pop()

            if isinstance(node, Func):
                context.function_depth -= 1
                if context.function_depth == 0:
                    context.in_function_args = False
            elif isinstance(node, exp.Case):
                context.in_case_when = False
            elif isinstance(node, Array):
                context.in_array = False
            elif isinstance(node, exp.In):
                context.in_in_clause = False

    def _process_literal_with_context(self, literal: exp.Literal, context: ParameterizationContext) -> exp.Expression:
        """Process a literal with awareness of its AST context."""
        # Check if this literal should be preserved based on context
        if self._should_preserve_literal_in_context(literal, context):
            return literal

        # Extract the literal value with type preservation
        literal_value, literal_type = self._extract_literal_value_and_type(literal)

        # Add to parameters list with metadata
        self.extracted_parameters.append(literal_value)
        self._parameter_metadata.append(
            {
                "index": len(self.extracted_parameters) - 1,
                "type": literal_type,
                "original_sql": literal.sql(),
                "context": self._get_context_description(context),
            }
        )

        # Create appropriate placeholder
        return self._create_placeholder()

    def _should_preserve_literal_in_context(self, literal: exp.Literal, context: ParameterizationContext) -> bool:
        """Context-aware decision on literal preservation."""
        # Check for NULL values
        if self.preserve_null and isinstance(literal, Null):
            return True

        # Check for boolean values
        if self.preserve_boolean and isinstance(literal, Boolean):
            return True

        # Check if in preserved function arguments
        if context.in_function_args:
            return True

        # Check parent context more intelligently
        for parent in context.parent_stack:
            # Preserve in schema/DDL contexts
            if isinstance(parent, (DataType, exp.ColumnDef, exp.Create, exp.Schema)):
                return True

            # Preserve numbers in LIMIT/OFFSET
            if (
                self.preserve_numbers_in_limit
                and isinstance(parent, (exp.Limit, exp.Offset))
                and self._is_number_literal(literal)
            ):
                return True

            # Preserve in CASE conditions for readability
            if isinstance(parent, exp.Case) and context.in_case_when:
                # Only preserve simple comparisons
                return not isinstance(literal.parent, Binary)

        # Check string length
        if self._is_string_literal(literal):
            string_value = str(literal.this)
            if len(string_value) > self.max_string_length:
                return True

        return False

    def _extract_literal_value_and_type(self, literal: exp.Literal) -> tuple[Any, str]:
        """Extract the Python value and type info from a SQLGlot literal."""
        if isinstance(literal, Null) or literal.this is None:
            return None, "null"

        if isinstance(literal, Boolean) or isinstance(literal.this, bool):
            return literal.this, "boolean"

        if self._is_string_literal(literal):
            return str(literal.this), "string"

        if self._is_number_literal(literal):
            # Preserve numeric precision if enabled
            if self.type_preservation:
                value_str = str(literal.this)
                if "." in value_str or "e" in value_str.lower():
                    try:
                        # Check if it's a decimal that needs precision
                        decimal_places = len(value_str.split(".")[1]) if "." in value_str else 0
                        if decimal_places > MAX_DECIMAL_PRECISION:  # Likely needs decimal precision
                            return value_str, "decimal"
                        return float(literal.this), "float"
                    except (ValueError, IndexError):
                        return str(literal.this), "numeric_string"
                else:
                    try:
                        value = int(literal.this)
                    except ValueError:
                        return str(literal.this), "numeric_string"
                    else:
                        # Check for bigint
                        if abs(value) > MAX_INT32_VALUE:  # Max 32-bit int
                            return value, "bigint"
                        return value, "integer"
            else:
                # Simple type conversion
                try:
                    if "." in str(literal.this):
                        return float(literal.this), "float"
                    return int(literal.this), "integer"
                except ValueError:
                    return str(literal.this), "numeric_string"

        # Handle date/time literals - these are DataType attributes not Literal attributes
        # Date/time values are typically string literals that need context-aware processing
        # We'll return them as strings and let the database handle type conversion

        # Fallback
        return str(literal.this), "unknown"

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

    def _create_placeholder(self, hint: Optional[str] = None) -> exp.Expression:
        """Create a placeholder expression with optional type hint."""
        self._parameter_counter += 1

        if self.placeholder_style == "?":
            placeholder = exp.Placeholder()
        elif self.placeholder_style == ":name":
            # Use hint in parameter name if available
            param_name = f"{hint}_{self._parameter_counter}" if hint else f"param_{self._parameter_counter}"
            placeholder = exp.Placeholder(this=param_name)
        elif self.placeholder_style.startswith(":"):
            param_name = f"param_{self._parameter_counter}"
            placeholder = exp.Placeholder(this=param_name)
        elif self.placeholder_style.startswith("$"):
            # PostgreSQL style numbered parameters
            placeholder = exp.Placeholder(this=f"${self._parameter_counter}")
        else:
            # Default to question mark
            placeholder = exp.Placeholder()

        return placeholder

    def _process_array(self, array_node: Array, context: ParameterizationContext) -> exp.Expression:
        """Process array literals for parameterization."""
        if not array_node.expressions:
            return array_node

        # Check array size
        if len(array_node.expressions) > self.max_array_length:
            # Too large, preserve as-is
            return array_node

        # Extract all array elements
        array_values = []
        all_literals = True

        for expr in array_node.expressions:
            if isinstance(expr, Literal):
                value, _ = self._extract_literal_value_and_type(expr)
                array_values.append(value)
            else:
                all_literals = False
                break

        if all_literals:
            # Replace entire array with a single parameter
            self.extracted_parameters.append(array_values)
            self._parameter_metadata.append(
                {
                    "index": len(self.extracted_parameters) - 1,
                    "type": "array",
                    "length": len(array_values),
                    "context": "array_literal",
                }
            )
            return self._create_placeholder("array")
        # Process individual elements
        new_expressions = []
        for expr in array_node.expressions:
            if isinstance(expr, Literal):
                new_expressions.append(self._process_literal_with_context(expr, context))
            else:
                new_expressions.append(self._transform_with_context(expr, context))
        array_node.expressions = new_expressions
        return array_node

    def _process_in_clause(self, in_node: exp.In, context: ParameterizationContext) -> exp.Expression:
        """Process IN clause for intelligent parameterization."""
        # Check if it's a subquery IN clause (has 'query' in args)
        if in_node.args.get("query"):
            # Don't parameterize subqueries, just process them recursively
            in_node.set("query", self._transform_with_context(in_node.args["query"], context))
            return in_node

        # Check if it has literal expressions (the values on the right side)
        if "expressions" not in in_node.args or not in_node.args["expressions"]:
            return in_node

        # Check if the IN list is too large
        expressions = in_node.args["expressions"]
        if len(expressions) > self.max_in_list_size:
            # Consider alternative strategies for large IN lists
            return in_node

        # Process the expressions in the IN clause
        has_literals = any(isinstance(expr, Literal) for expr in expressions)

        if has_literals:
            # Transform literals in the IN list
            new_expressions = []
            for expr in expressions:
                if isinstance(expr, Literal):
                    new_expressions.append(self._process_literal_with_context(expr, context))
                else:
                    new_expressions.append(self._transform_with_context(expr, context))

            # Update the IN node's expressions using set method
            in_node.set("expressions", new_expressions)

        return in_node

    def _get_context_description(self, context: ParameterizationContext) -> str:
        """Get a description of the current parameterization context."""
        descriptions = []

        if context.in_function_args:
            descriptions.append("function_args")
        if context.in_case_when:
            descriptions.append("case_when")
        if context.in_array:
            descriptions.append("array")
        if context.in_in_clause:
            descriptions.append("in_clause")

        if not descriptions:
            # Try to determine from parent stack
            for parent in reversed(context.parent_stack):
                if isinstance(parent, exp.Select):
                    descriptions.append("select")
                    break
                if isinstance(parent, exp.Where):
                    descriptions.append("where")
                    break
                if isinstance(parent, exp.Join):
                    descriptions.append("join")
                    break

        return "_".join(descriptions) if descriptions else "general"

    def get_parameters(self) -> list[Any]:
        """Get the list of extracted parameters from the last processing operation.

        Returns:
            List of parameter values extracted during the last process() call.
        """
        return self.extracted_parameters.copy()

    def get_parameter_metadata(self) -> list[dict[str, Any]]:
        """Get metadata about extracted parameters for advanced usage.

        Returns:
            List of parameter metadata dictionaries.
        """
        return self._parameter_metadata.copy()

    def clear_parameters(self) -> None:
        """Clear the extracted parameters list."""
        self.extracted_parameters = []
        self._parameter_counter = 0
        self._parameter_metadata = []
