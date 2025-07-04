"""Replaces literals in SQL with placeholders and extracts them using SQLGlot AST."""

from dataclasses import dataclass
from typing import Any, Optional, Union

from sqlglot import exp
from sqlglot.expressions import Array, Binary, Boolean, DataType, Func, Literal, Null

from sqlspec.protocols import ProcessorProtocol
from sqlspec.statement.parameters import ParameterStyle, TypedParameter
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

MAX_ENUM_LENGTH = 50
"""Maximum length for enum-like string values."""

MIN_ENUM_LENGTH = 2
"""Minimum length for enum-like string values to be meaningful."""


@dataclass
class ParameterizationContext:
    """Context for tracking parameterization state during AST traversal."""

    parent_stack: list[exp.Expression]
    in_function_args: bool = False
    in_case_when: bool = False
    in_array: bool = False
    in_in_clause: bool = False
    in_recursive_cte: bool = False
    in_subquery: bool = False
    in_select_list: bool = False
    in_join_condition: bool = False
    function_depth: int = 0
    cte_depth: int = 0
    subquery_depth: int = 0


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
        preserve_in_recursive_cte: Whether to preserve literals in recursive CTEs (default True to avoid type inference issues).
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
        preserve_in_recursive_cte: bool = True,
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
        self.preserve_in_recursive_cte = preserve_in_recursive_cte
        self.preserve_in_functions = preserve_in_functions or [
            "COALESCE",
            "IFNULL",
            "NVL",
            "ISNULL",
            # Array functions that take dimension arguments
            "ARRAYSIZE",  # SQLglot converts array_length to ArraySize
            "ARRAY_UPPER",
            "ARRAY_LOWER",
            "ARRAY_NDIMS",
            "ROUND",
        ]
        self.parameterize_arrays = parameterize_arrays
        self.parameterize_in_lists = parameterize_in_lists
        self.max_string_length = max_string_length
        self.max_array_length = max_array_length
        self.max_in_list_size = max_in_list_size
        self.type_preservation = type_preservation
        self.extracted_parameters: list[Any] = []
        self._parameter_counter = 0
        self._parameter_metadata: list[dict[str, Any]] = []  # Track parameter types and context
        self._preserve_dict_format = False  # Track whether to preserve dict format

    def process(self, expression: Optional[exp.Expression], context: SQLProcessingContext) -> Optional[exp.Expression]:
        """Advanced literal parameterization with context-aware AST analysis."""
        if expression is None or context.current_expression is None:
            return expression

        # For named parameters (like BigQuery @param), don't reorder to avoid breaking name mapping
        if (
            context.config.input_sql_had_placeholders
            and context.parameter_info
            and any(p.name for p in context.parameter_info)
        ):
            return expression

        self.extracted_parameters = []
        self._parameter_metadata = []

        # When reordering is needed (SQL already has placeholders), we need to start
        # our counter at the number of existing parameters to avoid conflicts
        if context.config.input_sql_had_placeholders and context.parameter_info:
            # Find the highest ordinal among existing parameters
            max_ordinal = max(p.ordinal for p in context.parameter_info)
            self._parameter_counter = max_ordinal + 1
        else:
            self._parameter_counter = 0

        # Track original user parameters for proper merging
        self._original_params = context.merged_parameters
        self._user_param_index = 0
        # If original params are dict and we have named placeholders, preserve dict format
        if isinstance(context.merged_parameters, dict) and context.parameter_info:
            # Check if we have named placeholders
            has_named = any(p.name for p in context.parameter_info)
            if has_named:
                self._final_params: Union[dict[str, Any], list[Any]] = {}
                self._preserve_dict_format = True
            else:
                self._final_params = []
                self._preserve_dict_format = False
        else:
            self._final_params = []
            self._preserve_dict_format = False
        self._is_reordering_needed = context.config.input_sql_had_placeholders

        param_context = ParameterizationContext(parent_stack=[])
        transformed_expression = self._transform_with_context(context.current_expression.copy(), param_context)
        context.current_expression = transformed_expression

        # If we're reordering, update the merged parameters with the reordered result
        # In this case, we don't need to add to extracted_parameters_from_pipeline
        # because the parameters are already in _final_params
        if self._is_reordering_needed and self._final_params:
            context.merged_parameters = self._final_params
        else:
            # Only add extracted parameters to the pipeline if we're not reordering
            # This prevents duplication when parameters are already in merged_parameters
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
        elif isinstance(node, exp.Placeholder) and self._is_reordering_needed:
            # Handle existing placeholders when reordering is needed
            result = self._process_existing_placeholder(node, context)
        elif isinstance(node, exp.Parameter) and self._is_reordering_needed:
            # Handle PostgreSQL-style parameters ($1, $2) when reordering is needed
            result = self._process_existing_parameter(node, context)
        elif isinstance(node, exp.Column) and self._is_reordering_needed:
            # Check if this column looks like a PostgreSQL parameter ($1, $2, etc.)
            column_name = str(node.this) if hasattr(node, "this") else ""
            if column_name.startswith("$") and column_name[1:].isdigit():
                # This is a PostgreSQL-style parameter parsed as a column
                result = self._process_postgresql_column_parameter(node, context)
            else:
                # Regular column - process children
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
            self._update_context_entering(node, context)
        else:
            self._update_context_leaving(node, context)

    def _update_context_entering(self, node: exp.Expression, context: ParameterizationContext) -> None:
        """Update context when entering a node."""
        context.parent_stack.append(node)

        if isinstance(node, Func):
            self._update_context_entering_func(node, context)
        elif isinstance(node, exp.Case):
            context.in_case_when = True
        elif isinstance(node, Array):
            context.in_array = True
        elif isinstance(node, exp.In):
            context.in_in_clause = True
        elif isinstance(node, exp.CTE):
            self._update_context_entering_cte(node, context)
        elif isinstance(node, exp.Subquery):
            context.subquery_depth += 1
            context.in_subquery = True
        elif isinstance(node, exp.Select):
            self._update_context_entering_select(node, context)
        elif isinstance(node, exp.Join):
            context.in_join_condition = True

    def _update_context_entering_func(self, node: Func, context: ParameterizationContext) -> None:
        """Update context when entering a function node."""
        context.function_depth += 1
        # Get function name from class name or node.name
        func_name = node.__class__.__name__.upper()
        if func_name in self.preserve_in_functions or (node.name and node.name.upper() in self.preserve_in_functions):
            context.in_function_args = True

    def _update_context_entering_cte(self, node: exp.CTE, context: ParameterizationContext) -> None:
        """Update context when entering a CTE node."""
        context.cte_depth += 1
        # Check if this CTE is recursive:
        # 1. Parent WITH must be RECURSIVE
        # 2. CTE must contain UNION (characteristic of recursive CTEs)
        is_in_recursive_with = any(
            isinstance(parent, exp.With) and parent.args.get("recursive", False)
            for parent in reversed(context.parent_stack)
        )
        if is_in_recursive_with and self._contains_union(node):
            context.in_recursive_cte = True

    def _update_context_entering_select(self, node: exp.Select, context: ParameterizationContext) -> None:
        """Update context when entering a SELECT node."""
        # Only track nested SELECT statements as subqueries if they're not part of a recursive CTE
        is_in_recursive_cte = any(
            isinstance(parent, exp.CTE)
            and any(
                isinstance(grandparent, exp.With) and grandparent.args.get("recursive", False)
                for grandparent in context.parent_stack
            )
            for parent in context.parent_stack[:-1]
        )

        if not is_in_recursive_cte and any(
            isinstance(parent, (exp.Select, exp.Subquery, exp.CTE))
            for parent in context.parent_stack[:-1]  # Exclude the current node
        ):
            context.subquery_depth += 1
            context.in_subquery = True
        # Check if we're in a SELECT clause expressions list
        if hasattr(node, "expressions"):
            # We'll handle this specifically when processing individual expressions
            context.in_select_list = False  # Will be detected by _is_in_select_expressions

    def _update_context_leaving(self, node: exp.Expression, context: ParameterizationContext) -> None:
        """Update context when leaving a node."""
        if context.parent_stack:
            context.parent_stack.pop()

        if isinstance(node, Func):
            self._update_context_leaving_func(node, context)
        elif isinstance(node, exp.Case):
            context.in_case_when = False
        elif isinstance(node, Array):
            context.in_array = False
        elif isinstance(node, exp.In):
            context.in_in_clause = False
        elif isinstance(node, exp.CTE):
            self._update_context_leaving_cte(node, context)
        elif isinstance(node, exp.Subquery):
            self._update_context_leaving_subquery(node, context)
        elif isinstance(node, exp.Select):
            self._update_context_leaving_select(node, context)
        elif isinstance(node, exp.Join):
            context.in_join_condition = False

    def _update_context_leaving_func(self, node: Func, context: ParameterizationContext) -> None:
        """Update context when leaving a function node."""
        context.function_depth -= 1
        if context.function_depth == 0:
            context.in_function_args = False

    def _update_context_leaving_cte(self, node: exp.CTE, context: ParameterizationContext) -> None:
        """Update context when leaving a CTE node."""
        context.cte_depth -= 1
        if context.cte_depth == 0:
            context.in_recursive_cte = False

    def _update_context_leaving_subquery(self, node: exp.Subquery, context: ParameterizationContext) -> None:
        """Update context when leaving a subquery node."""
        context.subquery_depth -= 1
        if context.subquery_depth == 0:
            context.in_subquery = False

    def _update_context_leaving_select(self, node: exp.Select, context: ParameterizationContext) -> None:
        """Update context when leaving a SELECT node."""
        # Only decrement if this was a nested SELECT (not part of recursive CTE)
        is_in_recursive_cte = any(
            isinstance(parent, exp.CTE)
            and any(
                isinstance(grandparent, exp.With) and grandparent.args.get("recursive", False)
                for grandparent in context.parent_stack
            )
            for parent in context.parent_stack[:-1]
        )

        if not is_in_recursive_cte and any(
            isinstance(parent, (exp.Select, exp.Subquery, exp.CTE))
            for parent in context.parent_stack[:-1]  # Exclude current node
        ):
            context.subquery_depth -= 1
            if context.subquery_depth == 0:
                context.in_subquery = False
        context.in_select_list = False

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

        # Always track extracted parameters for proper merging
        self.extracted_parameters.append(typed_param)

        # If we're reordering, also add to final params directly
        if self._is_reordering_needed:
            if self._preserve_dict_format and isinstance(self._final_params, dict):
                # For dict format, we need a key
                param_key = semantic_name or f"param_{len(self._final_params)}"
                self._final_params[param_key] = typed_param
            elif isinstance(self._final_params, list):
                self._final_params.append(typed_param)
            else:
                # Fallback - this shouldn't happen but handle gracefully
                if not hasattr(self, "_fallback_params"):
                    self._fallback_params = []
                self._fallback_params.append(typed_param)

        self._parameter_metadata.append(
            {
                "index": len(self._final_params if self._is_reordering_needed else self.extracted_parameters) - 1,
                "type": type_hint,
                "semantic_name": semantic_name,
                "context": self._get_context_description(context),
            }
        )

        # Create appropriate placeholder
        return self._create_placeholder(hint=semantic_name)

    def _process_existing_placeholder(self, node: exp.Placeholder, context: ParameterizationContext) -> exp.Expression:
        """Process an existing placeholder when reordering parameters."""
        if self._original_params is None:
            return node

        if isinstance(self._original_params, (list, tuple)):
            self._handle_list_params_for_placeholder(node)
        elif isinstance(self._original_params, dict):
            self._handle_dict_params_for_placeholder(node)
        else:
            # Single value parameter
            self._handle_single_value_param_for_placeholder(node)

        return node

    def _handle_list_params_for_placeholder(self, node: exp.Placeholder) -> None:
        """Handle list/tuple parameters for placeholder."""
        if isinstance(self._original_params, (list, tuple)) and self._user_param_index < len(self._original_params):
            value = self._original_params[self._user_param_index]
            self._add_to_final_params(value, node)
            self._user_param_index += 1
        else:
            # More placeholders than user parameters
            self._add_to_final_params(None, node)

    def _handle_dict_params_for_placeholder(self, node: exp.Placeholder) -> None:
        """Handle dict parameters for placeholder."""
        if not isinstance(self._original_params, dict):
            self._add_to_final_params(None, node)
            return

        raw_placeholder_name = node.this if hasattr(node, "this") else None
        if not raw_placeholder_name:
            # Unnamed placeholder '?' with dict params is ambiguous
            self._add_to_final_params(None, node)
            return

        # FIX: Normalize the placeholder name by stripping leading sigils
        placeholder_name = raw_placeholder_name.lstrip(":@")

        # Debug logging

        if placeholder_name in self._original_params:
            # Direct match for placeholder name
            self._add_to_final_params(self._original_params[placeholder_name], node)
        elif placeholder_name.isdigit() and self._user_param_index == 0:
            # Oracle-style numeric parameters
            self._handle_oracle_numeric_params()
            self._user_param_index += 1
        elif placeholder_name.isdigit() and self._user_param_index > 0:
            # Already handled Oracle params
            pass
        elif self._user_param_index == 0 and len(self._original_params) > 0:
            # Single dict parameter case
            self._handle_single_dict_param()
            self._user_param_index += 1
        else:
            # No match found
            self._add_to_final_params(None, node)

    def _handle_single_value_param_for_placeholder(self, node: exp.Placeholder) -> None:
        """Handle single value parameter for placeholder."""
        if self._user_param_index == 0:
            self._add_to_final_params(self._original_params, node)
            self._user_param_index += 1
        else:
            self._add_to_final_params(None, node)

    def _handle_oracle_numeric_params(self) -> None:
        """Handle Oracle-style numeric parameters."""
        if not isinstance(self._original_params, dict):
            return

        if self._preserve_dict_format and isinstance(self._final_params, dict):
            for k, v in self._original_params.items():
                if k.isdigit():
                    self._final_params[k] = v
        else:
            # Convert to positional list
            numeric_keys = [k for k in self._original_params if k.isdigit()]
            if numeric_keys:
                max_index = max(int(k) for k in numeric_keys)
                param_list = [None] * (max_index + 1)
                for k, v in self._original_params.items():
                    if k.isdigit():
                        param_list[int(k)] = v
                if isinstance(self._final_params, list):
                    self._final_params.extend(param_list)
                elif isinstance(self._final_params, dict):
                    for i, val in enumerate(param_list):
                        self._final_params[str(i)] = val

    def _handle_single_dict_param(self) -> None:
        """Handle single dict parameter case."""
        if not isinstance(self._original_params, dict):
            return

        if self._preserve_dict_format and isinstance(self._final_params, dict):
            for k, v in self._original_params.items():
                self._final_params[k] = v
        elif isinstance(self._final_params, list):
            self._final_params.append(self._original_params)
        elif isinstance(self._final_params, dict):
            param_name = f"param_{len(self._final_params)}"
            self._final_params[param_name] = self._original_params

    def _add_to_final_params(self, value: Any, node: exp.Placeholder) -> None:
        """Add a value to final params with proper type handling."""
        if self._preserve_dict_format and isinstance(self._final_params, dict):
            placeholder_name = node.this if hasattr(node, "this") else f"param_{self._user_param_index}"
            self._final_params[placeholder_name] = value
        elif isinstance(self._final_params, list):
            self._final_params.append(value)
        elif isinstance(self._final_params, dict):
            param_name = f"param_{len(self._final_params)}"
            self._final_params[param_name] = value

    def _process_existing_parameter(self, node: exp.Parameter, context: ParameterizationContext) -> exp.Expression:
        """Process existing parameters (both numeric and named) when reordering parameters."""
        # First try to get parameter name for named parameters (like BigQuery @param_name)
        param_name = self._extract_parameter_name(node)

        if param_name and isinstance(self._original_params, dict) and param_name in self._original_params:
            value = self._original_params[param_name]
            self._add_param_value_to_finals(value)
            return node

        # Fall back to numeric parameter handling for PostgreSQL-style parameters ($1, $2)
        param_index = self._extract_parameter_index(node)

        if self._original_params is None:
            self._add_none_to_final_params()
        elif isinstance(self._original_params, (list, tuple)):
            self._handle_list_params_for_parameter_node(param_index)
        elif isinstance(self._original_params, dict):
            self._handle_dict_params_for_parameter_node(param_index)
        elif param_index == 0:
            # Single parameter case
            self._add_param_value_to_finals(self._original_params)
        else:
            self._add_none_to_final_params()

        # Return the parameter unchanged
        return node

    @staticmethod
    def _extract_parameter_name(node: exp.Parameter) -> Optional[str]:
        """Extract parameter name from a Parameter node for named parameters."""
        if hasattr(node, "this"):
            if isinstance(node.this, exp.Var):
                # Named parameter like @min_value -> min_value
                return str(node.this.this)
            if hasattr(node.this, "this"):
                # Handle other node types that might contain the name
                return str(node.this.this)
        return None

    @staticmethod
    def _extract_parameter_index(node: exp.Parameter) -> Optional[int]:
        """Extract parameter index from a Parameter node."""
        if hasattr(node, "this") and isinstance(node.this, Literal):
            import contextlib

            with contextlib.suppress(ValueError, TypeError):
                return int(node.this.this) - 1  # Convert to 0-based index
        return None

    def _handle_list_params_for_parameter_node(self, param_index: Optional[int]) -> None:
        """Handle list/tuple parameters for Parameter node."""
        if (
            isinstance(self._original_params, (list, tuple))
            and param_index is not None
            and 0 <= param_index < len(self._original_params)
        ):
            # Use the parameter at the specified index
            self._add_param_value_to_finals(self._original_params[param_index])
        else:
            # More parameters than user provided
            self._add_none_to_final_params()

    def _handle_dict_params_for_parameter_node(self, param_index: Optional[int]) -> None:
        """Handle dict parameters for Parameter node."""
        if param_index is not None:
            self._handle_dict_param_with_index(param_index)
        else:
            self._add_none_to_final_params()

    def _handle_dict_param_with_index(self, param_index: int) -> None:
        """Handle dict parameter when we have an index."""
        if not isinstance(self._original_params, dict):
            self._add_none_to_final_params()
            return

        # Try param_N key first
        param_key = f"param_{param_index}"
        if param_key in self._original_params:
            self._add_dict_value_to_finals(param_key)
            return

        # Try direct numeric key (1-based)
        numeric_key = str(param_index + 1)
        if numeric_key in self._original_params:
            self._add_dict_value_to_finals(numeric_key)
        else:
            self._add_none_to_final_params()

    def _add_dict_value_to_finals(self, key: str) -> None:
        """Add a value from dict params to final params."""
        if isinstance(self._original_params, dict) and key in self._original_params:
            value = self._original_params[key]
            if isinstance(self._final_params, list):
                self._final_params.append(value)
            elif isinstance(self._final_params, dict):
                self._final_params[key] = value

    def _add_param_value_to_finals(self, value: Any) -> None:
        """Add a parameter value to final params."""
        if isinstance(self._final_params, list):
            self._final_params.append(value)
        elif isinstance(self._final_params, dict):
            param_name = f"param_{len(self._final_params)}"
            self._final_params[param_name] = value

    def _add_none_to_final_params(self) -> None:
        """Add None to final params."""
        if isinstance(self._final_params, list):
            self._final_params.append(None)
        elif isinstance(self._final_params, dict):
            param_name = f"param_{len(self._final_params)}"
            self._final_params[param_name] = None

    def _process_postgresql_column_parameter(
        self, node: exp.Column, context: ParameterizationContext
    ) -> exp.Expression:
        """Process PostgreSQL-style parameters that were parsed as columns ($1, $2)."""
        # Extract the numeric part from $1, $2, etc.
        column_name = str(node.this) if hasattr(node, "this") else ""
        param_index = None

        if column_name.startswith("$") and column_name[1:].isdigit():
            import contextlib

            with contextlib.suppress(ValueError, TypeError):
                param_index = int(column_name[1:]) - 1  # Convert to 0-based index

        if self._original_params is None:
            # No user parameters provided - don't add None
            return node
        if isinstance(self._original_params, (list, tuple)):
            # When we have mixed parameter styles and reordering is needed,
            # use sequential assignment based on _user_param_index
            if self._is_reordering_needed:
                # For mixed styles, parameters should be assigned sequentially
                # regardless of the numeric value in the placeholder
                if self._user_param_index < len(self._original_params):
                    param_value = self._original_params[self._user_param_index]
                    self._user_param_index += 1
                else:
                    param_value = None
            else:
                # Non-mixed styles - use the numeric value from the placeholder
                param_value = (
                    self._original_params[param_index]
                    if param_index is not None and 0 <= param_index < len(self._original_params)
                    else None
                )

            if param_value is not None:
                # Add the parameter value to final params
                if self._preserve_dict_format and isinstance(self._final_params, dict):
                    param_key = f"param_{len(self._final_params)}"
                    self._final_params[param_key] = param_value
                elif isinstance(self._final_params, list):
                    self._final_params.append(param_value)
                elif isinstance(self._final_params, dict):
                    param_name = f"param_{len(self._final_params)}"
                    self._final_params[param_name] = param_value
            # More parameters than user provided - don't add None
        elif isinstance(self._original_params, dict):
            # For dict parameters with numeric placeholders, try to map by index
            if param_index is not None:
                param_key = f"param_{param_index}"
                if param_key in self._original_params:
                    if self._preserve_dict_format and isinstance(self._final_params, dict):
                        self._final_params[param_key] = self._original_params[param_key]
                    elif isinstance(self._final_params, list):
                        self._final_params.append(self._original_params[param_key])
                    elif isinstance(self._final_params, dict):
                        self._final_params[param_key] = self._original_params[param_key]
                else:
                    # Try direct numeric key
                    numeric_key = str(param_index + 1)  # 1-based
                    if numeric_key in self._original_params:
                        if self._preserve_dict_format and isinstance(self._final_params, dict):
                            self._final_params[numeric_key] = self._original_params[numeric_key]
                        elif isinstance(self._final_params, list):
                            self._final_params.append(self._original_params[numeric_key])
                        elif isinstance(self._final_params, dict):
                            self._final_params[numeric_key] = self._original_params[numeric_key]
        # Single parameter case
        elif param_index == 0:
            if self._preserve_dict_format and isinstance(self._final_params, dict):
                param_key = f"param_{len(self._final_params)}"
                self._final_params[param_key] = self._original_params
            elif isinstance(self._final_params, list):
                self._final_params.append(self._original_params)
            elif isinstance(self._final_params, dict):
                param_name = f"param_{len(self._final_params)}"
                self._final_params[param_name] = self._original_params

        # Return the column unchanged - it represents the parameter placeholder
        return node

    def _should_preserve_literal_in_context(self, literal: exp.Expression, context: ParameterizationContext) -> bool:
        """Enhanced context-aware decision on literal preservation."""
        # Existing preservation rules (maintain compatibility)
        if self.preserve_null and isinstance(literal, Null):
            return True

        if self.preserve_boolean and isinstance(literal, Boolean):
            return True

        # NEW: Context-based preservation rules

        # Rule 4: Preserve enum-like literals in subquery lookups (the main fix we need)
        if context.in_subquery and self._is_scalar_lookup_pattern(literal, context):
            return self._is_enum_like_literal(literal)

        # Existing preservation rules continue...

        # Check if in preserved function arguments
        if context.in_function_args:
            return True

        # ENHANCED: Intelligent recursive CTE literal preservation
        if self.preserve_in_recursive_cte and context.in_recursive_cte:
            return self._should_preserve_literal_in_recursive_cte(literal, context)

        # Check if this literal is being used as an alias value in SELECT
        # e.g., 'computed' as process_status should be preserved
        if hasattr(literal, "parent") and literal.parent:
            parent = literal.parent
            # Check if it's an Alias node and the literal is the expression (not the alias name)
            if isinstance(parent, exp.Alias) and parent.this == literal:
                # Check if this alias is in a SELECT clause
                for ancestor in context.parent_stack:
                    if isinstance(ancestor, exp.Select):
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

    def _is_in_select_expressions(self, literal: exp.Expression, context: ParameterizationContext) -> bool:
        """Check if literal is in SELECT clause expressions (critical for type inference)."""
        for parent in reversed(context.parent_stack):
            if isinstance(parent, exp.Select):
                if hasattr(parent, "expressions") and parent.expressions:
                    return any(self._literal_is_in_expression_tree(literal, expr) for expr in parent.expressions)
            elif isinstance(parent, (exp.Where, exp.Having, exp.Join)):
                return False
        return False

    def _is_recursive_computation(self, literal: exp.Expression, context: ParameterizationContext) -> bool:
        """Check if literal is part of recursive computation logic."""
        # Look for arithmetic operations that are part of recursive logic
        for parent in reversed(context.parent_stack):
            if isinstance(parent, exp.Binary) and parent.key in ("ADD", "SUB", "MUL", "DIV"):
                # Check if this arithmetic is in a SELECT clause of a recursive part
                return self._is_in_select_expressions(literal, context)
        return False

    def _should_preserve_literal_in_recursive_cte(
        self, literal: exp.Expression, context: ParameterizationContext
    ) -> bool:
        """Intelligent recursive CTE literal preservation based on semantic role."""
        # Preserve SELECT clause literals (type inference critical)
        if self._is_in_select_expressions(literal, context):
            return True

        # Preserve recursive computation literals (core logic)
        return self._is_recursive_computation(literal, context)

    def _literal_is_in_expression_tree(self, target_literal: exp.Expression, expr: exp.Expression) -> bool:
        """Check if target literal is within the given expression tree."""
        if expr == target_literal:
            return True
        # Recursively check child expressions
        return any(child == target_literal for child in expr.iter_expressions())

    def _is_scalar_lookup_pattern(self, literal: exp.Expression, context: ParameterizationContext) -> bool:
        """Detect if literal is part of a scalar subquery lookup pattern."""
        # Must be in a subquery for this pattern to apply
        if context.subquery_depth == 0:
            return False

        # Check if we're in a WHERE clause of a subquery that returns a single column
        # and the literal is being compared against a column
        for parent in reversed(context.parent_stack):
            if isinstance(parent, exp.Where):
                # Look for pattern: WHERE column = 'literal'
                if isinstance(parent.this, exp.Binary) and parent.this.right == literal:
                    return isinstance(parent.this.left, exp.Column)
                # Also check for literal on the left side: WHERE 'literal' = column
                if isinstance(parent.this, exp.Binary) and parent.this.left == literal:
                    return isinstance(parent.this.right, exp.Column)
        return False

    def _is_enum_like_literal(self, literal: exp.Expression) -> bool:
        """Detect if literal looks like an enum/identifier constant."""
        if not isinstance(literal, exp.Literal) or not self._is_string_literal(literal):
            return False

        value = str(literal.this)

        # Conservative heuristics for enum-like values
        return (
            len(value) <= MAX_ENUM_LENGTH  # Reasonable length limit
            and value.replace("_", "").isalnum()  # Only alphanumeric + underscores
            and not value.isdigit()  # Not a pure number
            and len(value) > MIN_ENUM_LENGTH  # Not too short to be meaningful
        )

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

    def _contains_union(self, cte_node: exp.CTE) -> bool:
        """Check if a CTE contains a UNION (characteristic of recursive CTEs)."""

        def has_union(node: exp.Expression) -> bool:
            if isinstance(node, exp.Union):
                return True
            return any(has_union(child) for child in node.iter_expressions())

        return cte_node.this and has_union(cte_node.this)

    def clear_parameters(self) -> None:
        """Clear the extracted parameters list."""
        self.extracted_parameters = []
        self._parameter_counter = 0
        self._parameter_metadata = []
