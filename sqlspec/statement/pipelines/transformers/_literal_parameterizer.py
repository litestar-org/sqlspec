"""Replaces literals in SQL with placeholders and extracts them using SQLGlot AST."""

from dataclasses import dataclass
from typing import Any, Optional

from sqlglot import exp
from sqlglot.expressions import Array, Binary, Boolean, DataType, Func, Literal, Null

from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.pipelines.base import ProcessorProtocol
from sqlspec.statement.pipelines.context import SQLProcessingContext

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


class ParameterizeLiterals(ProcessorProtocol):
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

    def process(self, expression: Optional[exp.Expression], context: SQLProcessingContext) -> Optional[exp.Expression]:
        """Advanced literal parameterization with context-aware AST analysis."""
        if expression is None or context.current_expression is None or context.config.input_sql_had_placeholders:
            return expression

        self.extracted_parameters = []
        self._parameter_counter = 0
        self._parameter_metadata = []

        param_context = ParameterizationContext(parent_stack=[])
        transformed_expression = self._transform_with_context(context.current_expression.copy(), param_context)
        context.current_expression = transformed_expression
        context.extracted_parameters_from_pipeline.extend(self.extracted_parameters)

        context.metadata["parameter_metadata"] = self._parameter_metadata

        return transformed_expression

    def _transform_with_context(self, node: exp.Expression, context: ParameterizationContext) -> exp.Expression:
        """Transform expression tree with context tracking."""
        # Update context based on node type
        self._update_context(node, context, entering=True)

        # Process the node
        if isinstance(node, Literal):
            result = self._process_literal_with_context(node, context)
        elif isinstance(node, (Boolean, Null)):
            # Boolean and Null are not Literal subclasses, handle them separately
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

    def _process_literal_with_context(
        self, literal: exp.Expression, context: ParameterizationContext
    ) -> exp.Expression:
        """Process a literal with awareness of its AST context."""
        # Check if this literal should be preserved based on context
        if self._should_preserve_literal_in_context(literal, context):
            return literal

        # Use optimized extraction for single-pass processing
        value, type_hint, sqlglot_type, semantic_name = self._extract_literal_value_and_type_optimized(literal, context)

        # Create TypedParameter object
        from sqlspec.statement.parameters import TypedParameter

        typed_param = TypedParameter(
            value=value,
            sqlglot_type=sqlglot_type or exp.DataType.build("VARCHAR"),  # Fallback type
            type_hint=type_hint,
            semantic_name=semantic_name,
        )

        # Add to parameters list
        self.extracted_parameters.append(typed_param)
        self._parameter_metadata.append(
            {
                "index": len(self.extracted_parameters) - 1,
                "type": type_hint,
                "semantic_name": semantic_name,
                "context": self._get_context_description(context),
                # Note: We avoid calling literal.sql() for performance
            }
        )

        # Create appropriate placeholder
        return self._create_placeholder(hint=semantic_name)

    def _should_preserve_literal_in_context(self, literal: exp.Expression, context: ParameterizationContext) -> bool:
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
                and isinstance(literal, exp.Literal)
                and self._is_number_literal(literal)
            ):
                return True

            # Preserve in CASE conditions for readability
            if isinstance(parent, exp.Case) and context.in_case_when:
                # Only preserve simple comparisons
                return not isinstance(literal.parent, Binary)

        # Check string length
        if isinstance(literal, exp.Literal) and self._is_string_literal(literal):
            string_value = str(literal.this)
            if len(string_value) > self.max_string_length:
                return True

        return False

    def _extract_literal_value_and_type(self, literal: exp.Expression) -> tuple[Any, str]:
        """Extract the Python value and type info from a SQLGlot literal."""
        if isinstance(literal, Null) or literal.this is None:
            return None, "null"

        # Ensure we have a Literal for type checking methods
        if not isinstance(literal, exp.Literal):
            return str(literal), "string"

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

    def _extract_literal_value_and_type_optimized(
        self, literal: exp.Expression, context: ParameterizationContext
    ) -> "tuple[Any, str, Optional[exp.DataType], Optional[str]]":
        """Single-pass extraction of value, type hint, SQLGlot type, and semantic name.

        This optimized method extracts all information in one pass, avoiding redundant
        AST traversals and expensive operations like literal.sql().

        Args:
            literal: The literal expression to extract from
            context: Current parameterization context with parent stack

        Returns:
            Tuple of (value, type_hint, sqlglot_type, semantic_name)
        """
        # Extract value and basic type hint using existing logic
        value, type_hint = self._extract_literal_value_and_type(literal)

        # Determine SQLGlot type based on the type hint without additional parsing
        sqlglot_type = self._infer_sqlglot_type(type_hint, value)

        # Generate semantic name from context if available
        semantic_name = self._generate_semantic_name_from_context(literal, context)

        return value, type_hint, sqlglot_type, semantic_name

    @staticmethod
    def _infer_sqlglot_type(type_hint: str, value: Any) -> "Optional[exp.DataType]":
        """Infer SQLGlot DataType from type hint without parsing.

        Args:
            type_hint: The simple type hint string
            value: The actual value for additional context

        Returns:
            SQLGlot DataType instance or None
        """
        type_mapping = {
            "null": "NULL",
            "boolean": "BOOLEAN",
            "integer": "INT",
            "bigint": "BIGINT",
            "float": "FLOAT",
            "decimal": "DECIMAL",
            "string": "VARCHAR",
            "numeric_string": "VARCHAR",
            "unknown": "VARCHAR",
        }

        type_name = type_mapping.get(type_hint, "VARCHAR")

        # Build DataType with appropriate parameters
        if type_hint == "decimal" and isinstance(value, str):
            # Try to infer precision and scale
            parts = value.split(".")
            precision = len(parts[0]) + len(parts[1]) if len(parts) > 1 else len(parts[0])
            scale = len(parts[1]) if len(parts) > 1 else 0
            return exp.DataType.build(type_name, expressions=[exp.Literal.number(precision), exp.Literal.number(scale)])
        if type_hint == "string" and isinstance(value, str):
            # Infer VARCHAR length
            length = len(value)
            if length > 0:
                return exp.DataType.build(type_name, expressions=[exp.Literal.number(length)])

        # Default case - just the type name
        return exp.DataType.build(type_name)

    @staticmethod
    def _generate_semantic_name_from_context(
        literal: exp.Expression, context: ParameterizationContext
    ) -> "Optional[str]":
        """Generate semantic name from AST context using existing parent stack.

        Args:
            literal: The literal being parameterized
            context: Current context with parent stack

        Returns:
            Semantic name or None
        """
        # Look for column comparisons in parent stack
        for parent in reversed(context.parent_stack):
            if isinstance(parent, Binary):
                # It's a comparison - check if we're comparing to a column
                if parent.left == literal and isinstance(parent.right, exp.Column):
                    return parent.right.name
                if parent.right == literal and isinstance(parent.left, exp.Column):
                    return parent.left.name
            elif isinstance(parent, exp.In):
                # IN clause - check the left side for column
                if parent.this and isinstance(parent.this, exp.Column):
                    return f"{parent.this.name}_value"

        # Check if we're in a specific SQL clause
        for parent in reversed(context.parent_stack):
            if isinstance(parent, exp.Where):
                return "where_value"
            if isinstance(parent, exp.Having):
                return "having_value"
            if isinstance(parent, exp.Join):
                return "join_value"
            if isinstance(parent, exp.Select):
                return "select_value"

        return None

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
        # Import ParameterStyle for proper comparison

        # Handle both style names and actual placeholder prefixes
        style = self.placeholder_style
        if style in {"?", ParameterStyle.QMARK, "qmark"}:
            placeholder = exp.Placeholder()
        elif style == ":name":
            # Use hint in parameter name if available
            param_name = f"{hint}_{self._parameter_counter}" if hint else f"param_{self._parameter_counter}"
            placeholder = exp.Placeholder(this=param_name)
        elif style in {ParameterStyle.NAMED_COLON, "named_colon"} or style.startswith(":"):
            param_name = f"param_{self._parameter_counter}"
            placeholder = exp.Placeholder(this=param_name)
        elif style in {ParameterStyle.NUMERIC, "numeric"} or style.startswith("$"):
            # PostgreSQL style numbered parameters - use Var for consistent $N format
            # Note: PostgreSQL uses 1-based indexing
            placeholder = exp.Var(this=f"${self._parameter_counter + 1}")  # type: ignore[assignment]
        elif style in {ParameterStyle.NAMED_AT, "named_at"}:
            # BigQuery style @param - don't include @ in the placeholder name
            # The @ will be added during SQL generation
            # Use 0-based indexing for consistency with parameter arrays
            param_name = f"param_{self._parameter_counter}"
            placeholder = exp.Placeholder(this=param_name)
        elif style in {ParameterStyle.POSITIONAL_PYFORMAT, "pyformat"}:
            # Don't use pyformat directly in SQLGlot - use standard placeholder
            # and let the compile method convert it later
            placeholder = exp.Placeholder()
        else:
            # Default to question mark
            placeholder = exp.Placeholder()

        # Increment counter after creating placeholder
        self._parameter_counter += 1
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
        element_types = []
        all_literals = True

        for expr in array_node.expressions:
            if isinstance(expr, Literal):
                value, type_hint = self._extract_literal_value_and_type(expr)
                array_values.append(value)
                element_types.append(type_hint)
            else:
                all_literals = False
                break

        if all_literals:
            # Determine array element type from the first element
            element_type = element_types[0] if element_types else "unknown"

            # Create SQLGlot array type
            element_sqlglot_type = self._infer_sqlglot_type(element_type, array_values[0] if array_values else None)
            array_sqlglot_type = exp.DataType.build("ARRAY", expressions=[element_sqlglot_type])

            # Create TypedParameter for the entire array
            from sqlspec.statement.parameters import TypedParameter

            typed_param = TypedParameter(
                value=array_values,
                sqlglot_type=array_sqlglot_type,
                type_hint=f"array<{element_type}>",
                semantic_name="array_values",
            )

            # Replace entire array with a single parameter
            self.extracted_parameters.append(typed_param)
            self._parameter_metadata.append(
                {
                    "index": len(self.extracted_parameters) - 1,
                    "type": f"array<{element_type}>",
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
        array_node.set("expressions", new_expressions)
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
