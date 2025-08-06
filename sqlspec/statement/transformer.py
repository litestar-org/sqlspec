"""Single-pass SQL transformation with consolidated processing.

This module replaces the multi-stage pipeline architecture with a single
SQLGlot transform() call, eliminating redundant AST traversals and SQL generation.
"""

import threading
from typing import TYPE_CHECKING, Any, Callable, Optional

import sqlglot
import sqlglot.expressions as exp
from mypy_extensions import mypyc_attr

from sqlspec.parameters import ParameterConverter, ParameterStyle
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.statement.sql import StatementConfig

logger = get_logger(__name__)

__all__ = ("SQLTransformer",)


@mypyc_attr(allow_interpreted_subclasses=True)
class SQLTransformer:
    """Single-pass AST transformation with consolidated parameter processing.

    Replaces the entire pipeline infrastructure with a single SQLGlot transform()
    call for optimal performance. MyPyC compatible with __slots__ and typed attributes.

    Key optimizations:
    - Single AST traversal (vs multiple pipeline stages)
    - One SQL generation call (vs 3-4 calls in pipeline)
    - Consolidated parameter processing
    - Thread-safe with minimal locking overhead
    """

    __slots__ = (
        "_cached_result",
        "_context",
        "_hooks",
        "_lock",
        "_parameter_map",
        "config",
        "dialect",
        "parameter_style",
        "parameters",
    )

    def __init__(self, parameters: Any, dialect: str, config: "StatementConfig") -> None:
        self.parameters = parameters
        self.dialect = dialect
        self.config = config
        self._parameter_map: dict[str, Any] = {}
        self._context: dict[str, Any] = {}
        self._hooks = self._initialize_hooks()
        self._cached_result: Optional[tuple[str, Any]] = None
        self._lock = threading.Lock()  # Instance-level thread safety

        # Track original parameter format to preserve it in the output
        self.parameter_style = self._detect_original_format(parameters)

    def _initialize_hooks(self) -> "dict[str, list[Callable]]":
        """Initialize transformation hooks.

        Currently no built-in hooks are used, but users can extend this
        or provide hooks through custom configuration.
        """
        return {"pre_transform": [], "post_transform": []}

    def _detect_original_format(self, parameters: Any) -> str:
        """Detect the original parameter format to preserve it in output.

        Returns:
            'tuple' if original was tuple
            'list' if original was list
            'dict' if original was dict
            'other' for any other type
        """
        if isinstance(parameters, tuple):
            return "tuple"
        if isinstance(parameters, list):
            return "list"
        if isinstance(parameters, dict):
            return "dict"
        return "other"

    def transform(self, node: exp.Expression) -> exp.Expression:
        """Single-pass AST transformation with pre/post order processing.

        This method replaces the entire pipeline infrastructure:
        - Pre-order: Context collection, driver pre-processing
        - Post-order: Parameter processing, driver post-processing

        Optimized for minimal AST traversals and MyPyC compilation.
        """

        for hook in self._hooks["pre_transform"]:
            transformed = hook(node)
            if transformed is not None:
                node = transformed

        # Core parameter processing
        if isinstance(node, exp.Placeholder):
            # ADBC NULL parameter handling for PostgreSQL-style placeholders ($1, $2, etc.)
            if self.dialect == "postgres":
                null_replacement = self._replace_null_placeholder(node)
                node = null_replacement if null_replacement is not None else self._process_placeholder(node)
            else:
                node = self._process_placeholder(node)
        elif isinstance(node, exp.Parameter) and self.dialect == "postgres":
            # ADBC NULL parameter handling for generic parameter nodes
            null_replacement = self._replace_null_parameter(node)
            if null_replacement is not None:
                node = null_replacement

        # Post-order processing: run post-transform hooks
        for hook in self._hooks["post_transform"]:
            transformed = hook(node)
            if transformed is not None:
                node = transformed

        return node

    def _literalize_parameter(self, node: exp.Identifier) -> exp.Expression:
        """Convert parameter identifiers to literal values.

        Handles parameterization of identifiers that reference actual parameter values.
        Optimized for common parameter types with minimal type checking.
        """
        param_name = str(node)
        if param_name not in self.parameters:
            return node

        value = self.parameters[param_name]
        return self._create_literal_from_value(value)

    def _process_placeholder(self, node: exp.Placeholder) -> exp.Expression:
        """Process placeholder nodes for parameter substitution.

        Handles various placeholder formats ($1, ?, %(name)s) with
        optimized parameter lookup and type conversion.
        """
        placeholder_name = str(node.this) if hasattr(node, "this") else str(node)

        # Store placeholder for parameter mapping
        self._parameter_map[placeholder_name] = True

        # Return node unchanged - parameter substitution happens during SQL generation
        return node

    def _create_literal_from_value(self, value: Any) -> exp.Expression:
        """Create appropriate SQLGlot literal from Python value.

        Optimized for common types with minimal branching for MyPyC efficiency.
        """
        if value is None:
            return exp.Null()
        if isinstance(value, str):
            return exp.Literal.string(value)
        if isinstance(value, bool):
            return exp.Boolean(this=value)
        if isinstance(value, (int, float)):
            return exp.Literal.number(str(value))
        return exp.Literal.string(str(value))

    def _convert_parameter_styles(self, raw_sql: str) -> "tuple[str, list]":
        """Convert parameter styles only if they're incompatible with SQLGlot.

        Primary goal: Mirror the input format from SQLGlot unless it's unsupported.
        Only convert to execution_parameter_style when the current format is incompatible.

        Returns:
            Tuple of (converted_sql, parameter_info) for downstream processing
        """
        from sqlspec.parameters import ParameterValidator

        # Extract parameter information first
        validator = ParameterValidator()
        param_info = validator.extract_parameters(raw_sql)

        if not param_info:
            return raw_sql, []

        # Check if we need conversion for SQLGlot compatibility
        converter = ParameterConverter()
        incompatible_styles = converter.validator.get_sqlglot_incompatible_styles(self.dialect)
        detected_styles = {p.style for p in param_info}

        # Only convert if SQLGlot can't handle the current format
        needs_sqlglot_conversion = any(style in incompatible_styles for style in detected_styles)

        # IMPORTANT: We only convert for SQLGlot compatibility, NOT execution compatibility
        # The final execution style conversion happens later in _convert_to_execution_style
        # This preserves the original parameter styles when possible (e.g., %(name)s for psycopg)
        needs_conversion = needs_sqlglot_conversion

        if needs_conversion:
            # To match SQL class behavior exactly, use _convert_to_sqlglot_compatible
            # which always converts to :param_N format
            converted_sql = converter._convert_to_sqlglot_compatible(raw_sql, param_info)

            # For parameter conversion, we need to handle dict to list conversion
            # when the original params are dict but we're converting to positional style
            if isinstance(self.parameters, dict) and param_info:
                # Convert dict to list based on parameter order in SQL
                converted_params = []
                for p in param_info:
                    if p.name and p.name in self.parameters:
                        converted_params.append(self.parameters[p.name])
                    else:
                        # Fallback for unnamed parameters
                        converted_params.append(None)
                self._parameter_map["converted_params"] = converted_params
            else:
                self._parameter_map["converted_params"] = self.parameters

            new_param_info = validator.extract_parameters(converted_sql)
            return converted_sql, new_param_info

        # If we reach here, preserve the original format
        return raw_sql, param_info

    def _sqlglot_handles_natively(self, raw_sql: str, param_info: list) -> bool:
        """Test if SQLGlot can handle this SQL natively without parameter renaming.

        This is the performance-critical path - use SQLGlot's native capabilities
        when possible to avoid unnecessary ParameterConverter overhead.
        """
        try:
            # Parse and regenerate to test native handling
            parsed = sqlglot.parse_one(raw_sql, dialect=self.dialect)
            regenerated = parsed.sql(dialect=self.dialect)

            # If SQLGlot preserves the original SQL structure, use native handling
            # For positional parameters (name=None), check if placeholder text is preserved
            for p in param_info:
                if p.name is None:
                    # For positional parameters, check if placeholder text is preserved
                    if p.placeholder_text in regenerated:
                        continue
                    return False
                # For named parameters, check if parameter name is preserved
                if p.name in regenerated:
                    continue
                return False

        except Exception:
            # If parsing fails, we need ParameterConverter
            return False
        return True

    def _ensure_execution_compatibility(self, sql: str) -> "tuple[str, Any]":
        """Ensure SQL is in a format the driver can execute.

        This checks if the current parameter style is supported for execution.
        If not, it converts to a supported style.

        Args:
            sql: SQL with parameter placeholders

        Returns:
            Tuple of (final_sql, final_parameters)
        """
        from sqlspec.parameters import ParameterConverter, ParameterValidator

        # Check current parameter styles in the SQL
        validator = ParameterValidator()
        param_info = validator.extract_parameters(sql)

        if not param_info:
            # No parameters, just finalize and return
            final_params = self._finalize_parameters(sql)
            return sql, final_params

        current_styles = {p.style for p in param_info}

        # Check if current styles are supported for execution
        execution_styles = self.config.parameter_config.supported_execution_parameter_styles

        if execution_styles is not None:
            # Check if ALL current styles are supported
            if all(style in execution_styles for style in current_styles):
                # Already in a supported execution style
                final_params = self._finalize_parameters(sql)
                return sql, final_params

            # Need to convert to a supported execution style
            # Choose the first supported style (could be smarter about this)
            target_style = next(iter(execution_styles))

            # Convert to target style
            converter = ParameterConverter()
            try:
                # Use converted parameters from earlier conversion if available
                source_params = self._parameter_map.get("converted_params", self.parameters)
                converted_sql, converted_params = converter.convert_placeholder_style(sql, source_params, target_style)
                # Update parameter map with execution-converted params
                self._parameter_map["execution_params"] = converted_params
                final_params = self._finalize_parameters(converted_sql)
                return converted_sql, final_params
            except Exception:
                # Conversion failed, use original
                final_params = self._finalize_parameters(sql)
                return sql, final_params
        else:
            # No specific execution styles defined, driver should handle any style
            # This is the default case for drivers that auto-detect parameter styles
            final_params = self._finalize_parameters(sql)
            return sql, final_params

    def compile(self, raw_sql: str) -> "tuple[str, Any]":
        """Single compilation method replacing entire pipeline.

        Pure AST approach using proper SQLGlot patterns:
        1. Convert incompatible parameter styles to SQLGlot-compatible ones
        2. Parse SQL once
        3. Transform AST once (single traversal)
        4. Generate SQL once with copy=False
        5. Process parameters efficiently

        Thread-safe with instance-level locking.
        """
        with self._lock:
            if self._cached_result:
                return self._cached_result

            try:
                # 1. Convert incompatible parameter styles for SQLGlot parsing
                converted_sql, _ = self._convert_parameter_styles(raw_sql)

                # 2. Parse SQL once - handle unparsable SQL gracefully
                parsed = sqlglot.parse_one(converted_sql, dialect=self.dialect)

                # 3. Transform AST once using proper SQLGlot patterns
                transformed = parsed.transform(self.transform)

                # 4. Generate SQL once with copy=False for performance
                generated_sql = transformed.sql(dialect=self.dialect, copy=False)

                # 5. Check if we need execution style conversion
                final_sql, final_parameters = self._ensure_execution_compatibility(generated_sql)

                # 6. Cache result
                self._cached_result = (final_sql, final_parameters)

            except sqlglot.ParseError:
                # SQLGlot cannot parse this SQL (e.g., complex PL/SQL, T-SQL blocks)
                # Return original SQL with original parameters for pass-through execution
                logger.debug("SQLGlot cannot parse SQL, using pass-through mode: %s", raw_sql[:100])
                final_parameters = self._finalize_parameters()
                self._cached_result = (raw_sql, final_parameters)

            return self._cached_result

    def _finalize_parameters(self, generated_sql: Optional[str] = None) -> Any:
        """Finalize parameters for driver compatibility.

        Applies dialect-specific parameter conversions with minimal overhead.
        Handles ADBC NULL parameter cleanup if required.
        Converts parameter format based on actual generated SQL style.

        Args:
            generated_sql: The SQL that was just generated, for parameter style detection
        """
        if "null_parameter_positions" in self._context:
            return self._convert_to_original_format(self._apply_without_null_parameters())

        # Use execution-converted parameters if available
        if "execution_params" in self._parameter_map:
            execution_params = self._parameter_map["execution_params"]
            final_parameters = self._convert_to_original_format(execution_params)
        # Otherwise use converted parameters from parameter style conversion
        elif "converted_params" in self._parameter_map:
            converted_params = self._parameter_map["converted_params"]
            final_parameters = self._convert_to_original_format(converted_params)
        else:
            final_parameters = (
                self.parameters if not self._parameter_map else self._convert_to_original_format(self.parameters)
            )

        # Note: We do NOT convert parameter format based on SQL style here.
        # The SQL class doesn't do this - it preserves the original parameter format
        # even when the SQL uses a different style (e.g., list params with :param_0 SQL).
        # This allows drivers to handle the mapping internally.

        # Return None for empty parameters to match SQL class behavior
        if not final_parameters:
            return None

        return final_parameters

    def _convert_to_original_format(self, parameters: Any) -> Any:
        """Convert parameters back to the original format.

        If the original parameters were tuple/list, convert dict results back to list.
        Otherwise, preserve the current format.
        """
        # If original format was positional and current result is dict, convert to list
        if self.parameter_style in {"tuple", "list"} and isinstance(parameters, dict):
            # Check for numeric keys or param_N keys
            if all(k.isdigit() for k in parameters):
                # Pure numeric keys - sort by number
                sorted_items = sorted(parameters.items(), key=lambda x: int(x[0]))
                result_list = [item[1] for item in sorted_items]
            elif all(k.startswith("param_") and k[6:].isdigit() for k in parameters):
                # param_N keys - extract number and sort
                sorted_items = sorted(parameters.items(), key=lambda x: int(x[0][6:]))
                result_list = [item[1] for item in sorted_items]
            else:
                # Can't convert - return as is
                return parameters

            # Return tuple if original was tuple, list if original was list
            return tuple(result_list) if self.parameter_style == "tuple" else result_list

        # For all other cases, return parameters unchanged
        return parameters

    def _is_parameter_null(self, param_name: str) -> bool:
        """Check if a parameter has a NULL value.

        Optimized for common parameter formats with minimal string operations.
        """
        if isinstance(self.parameters, dict):
            return self.parameters.get(param_name) is None
        if isinstance(self.parameters, (list, tuple)):
            try:
                # Extract numeric index from parameter name like '$1', '$2'
                if param_name.startswith("$"):
                    index = int(param_name[1:]) - 1  # $1 -> index 0
                    return index < len(self.parameters) and self.parameters[index] is None
            except (ValueError, IndexError):
                pass
        return False

    def _replace_null_placeholder(self, node: exp.Placeholder) -> "Optional[exp.Expression]":
        """Transform PostgreSQL-style placeholders ($1, $2, etc.) for NULL handling."""
        if not (hasattr(node, "this") and isinstance(node.this, str)):
            return None

        try:
            param_str = node.this.lstrip("$")
            param_num = int(param_str)
            param_index = param_num - 1

            # Check if this parameter is NULL
            if self._is_parameter_null_by_index(param_index):
                # Track NULL positions for parameter cleanup
                self._track_null_parameter(param_index)
                return exp.Null()

            # Calculate new parameter number after NULL removal
            new_param_num = self._calculate_renumbered_position(param_index, param_num)
            if new_param_num != param_num:
                return exp.Placeholder(this=f"${new_param_num}")

        except (ValueError, AttributeError):
            pass

        return None

    def _replace_null_parameter(self, node: exp.Parameter) -> "Optional[exp.Expression]":
        """Transform generic parameter nodes for NULL handling."""
        if not hasattr(node, "this"):
            return None

        try:
            param_str = str(node.this)
            param_num = int(param_str)
            param_index = param_num - 1

            # Check if this parameter is NULL
            if self._is_parameter_null_by_index(param_index):
                # Track NULL positions for parameter cleanup
                self._track_null_parameter(param_index)
                return exp.Null()

            # Calculate new parameter number after NULL removal
            new_param_num = self._calculate_renumbered_position(param_index, param_num)
            if new_param_num != param_num:
                return exp.Parameter(this=str(new_param_num))

        except (ValueError, AttributeError):
            pass

        return None

    def _is_parameter_null_by_index(self, param_index: int) -> bool:
        """Check if parameter at specific index is NULL."""
        if isinstance(self.parameters, (list, tuple)):
            return param_index < len(self.parameters) and self.parameters[param_index] is None
        if isinstance(self.parameters, dict):
            # Handle dict parameters with various key formats
            for key, value in self.parameters.items():
                if isinstance(key, str) and key.lstrip("$").isdigit():
                    try:
                        key_index = int(key.lstrip("$")) - 1
                        if key_index == param_index:
                            return value is None
                    except ValueError:
                        continue
                elif isinstance(key, int) and key == param_index:
                    return value is None
        return False

    def _track_null_parameter(self, param_index: int) -> None:
        """Track NULL parameter positions for cleanup."""
        null_positions = self._context.setdefault("null_parameter_positions", set())
        null_positions.add(param_index)

    def _calculate_renumbered_position(self, param_index: int, original_param_num: int) -> int:
        """Calculate new parameter position after NULL parameters are removed."""
        null_positions = self._context.get("null_parameter_positions", set())
        nulls_before = sum(1 for idx in null_positions if idx < param_index)
        return original_param_num - nulls_before

    def _apply_without_null_parameters(self) -> Any:
        """Remove NULL parameters from parameter list for ADBC compatibility."""
        null_positions = self._context.get("null_parameter_positions", set())

        if isinstance(self.parameters, (list, tuple)):
            # Remove parameters at NULL positions
            return [param for i, param in enumerate(self.parameters) if i not in null_positions]

        if isinstance(self.parameters, dict):
            # Handle dict parameters with renumbering
            cleaned_dict = {}
            param_keys = sorted(
                self.parameters.keys(),
                key=lambda k: int(k.lstrip("$")) if isinstance(k, str) and k.lstrip("$").isdigit() else 0,
            )

            new_param_num = 1
            for key in param_keys:
                if self.parameters[key] is not None:
                    cleaned_dict[str(new_param_num)] = self.parameters[key]
                    new_param_num += 1

            return cleaned_dict

        return self.parameters

    def _needs_parameter_format_conversion(self, execution_style: "ParameterStyle", parameters: Any) -> bool:
        """Check if parameter format conversion is needed for the execution style."""
        from sqlspec.parameters import ParameterStyle

        # Named styles require dict parameters, positional styles require list/tuple
        named_styles = {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
        }

        expects_dict = execution_style in named_styles
        is_dict = isinstance(parameters, dict)
        is_sequence = isinstance(parameters, (list, tuple))

        # Conversion needed if format doesn't match expectation
        return (expects_dict and not is_dict) or (not expects_dict and not is_sequence)

    def _convert_parameters_for_execution_style(
        self, execution_style: "ParameterStyle", parameters: Any, sql: Optional[str] = None
    ) -> Any:
        """Convert parameters to match the execution style format."""
        from sqlspec.parameters import ParameterStyle, ParameterValidator

        named_styles = {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
        }

        expects_dict = execution_style in named_styles

        if expects_dict and isinstance(parameters, (list, tuple)):
            # Convert tuple/list to dict for named styles
            # Use provided SQL or fallback to cached result
            sql_to_use = sql or (self._cached_result[0] if self._cached_result else None)
            if sql_to_use:
                try:
                    validator = ParameterValidator()
                    param_info = validator.extract_parameters(sql_to_use)
                    return {
                        p_info.name or f"param_{i}": value
                        for i, (p_info, value) in enumerate(zip(param_info, parameters))
                    }
                except Exception:
                    # Fallback to generic param names
                    return {f"param_{i}": value for i, value in enumerate(parameters)}

        elif not expects_dict and isinstance(parameters, dict):
            # Convert dict to tuple/list for positional styles (like NUMERIC)
            # Use provided SQL or fallback to cached result
            sql_to_use = sql or (self._cached_result[0] if self._cached_result else None)
            if sql_to_use:
                try:
                    validator = ParameterValidator()
                    param_info = validator.extract_parameters(sql_to_use)
                    # Create list ordered by parameter positions in SQL
                    result_list = []
                    for p_info in param_info:
                        if execution_style == ParameterStyle.NUMERIC and p_info.name and p_info.name.isdigit():
                            # For NUMERIC style ($1, $2), use the numeric order
                            param_key = p_info.name
                        else:
                            # Try to find parameter by original name
                            param_key = None
                            for key in parameters:
                                if key == p_info.name or str(key) == p_info.name:
                                    param_key = key
                                    break

                        if param_key is not None and param_key in parameters:
                            result_list.append(parameters[param_key])
                        elif parameters:
                            # Fallback: try first available parameter value
                            result_list.append(next(iter(parameters.values())))
                        else:
                            result_list.append(None)
                except Exception:
                    # Fallback to list of values in original order
                    return list(parameters.values())
                else:
                    return result_list

        return parameters

    def _detect_sql_parameter_style(self, sql: str) -> "Optional[Any]":
        """Detect the actual parameter style used in the generated SQL.

        Returns the ParameterStyle enum value that matches the SQL, or None if no parameters.
        """
        from sqlspec.parameters import ParameterValidator

        try:
            validator = ParameterValidator()
            param_info = validator.extract_parameters(sql)

            if not param_info:
                return None

            # Return the style of the first parameter (all should be consistent)
            return param_info[0].style

        except Exception:
            return None

    def get_transformation_metadata(self) -> "dict[str, Any]":
        """Get metadata collected during transformation.

        Provides access to context and transformation results for diagnostics.
        """
        return {
            "context": self._context,
            "parameter_map": self._parameter_map,
            "hooks_applied": len(self._hooks["pre_transform"]) + len(self._hooks["post_transform"]),
        }
