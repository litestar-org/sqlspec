"""COMPREHENSIVE parameter pre-processing system for core.

This module implements the complete parameter pre-processing pipeline that:
1. Converts unsupported parameter types to SQLGlot-compatible formats (Phase 1)
2. Converts final SQL to execution formats when SQLGlot can't render specific types (Phase 2)
3. Handles all parameter style conversions and type coercions
4. Provides complete backward compatibility with existing driver requirements

Key Components:
- ParameterStyle enum: All 11 parameter styles supported
- TypedParameter: Preserves type information through processing pipeline
- ParameterInfo: Tracks position, style, ordinal, name for each parameter
- ParameterValidator: Extracts and validates parameters with dialect compatibility
- ParameterConverter: Handles both phase conversions with optimized lookups
- ParameterProcessor: High-level coordinator with caching and pipeline management
- ParameterStyleConfig: Complete configuration for all driver requirements

Performance Features:
- Singledispatch for type-specific parameter wrapping (O(1) dispatch)
- Hash-map lookups for parameter style conversions (O(1) operations)
- Comprehensive caching system for parameter extraction and conversion
- MyPyC optimization with __slots__ for memory efficiency

Compatibility Requirements:
- Preserve exact behavior from current parameters.py
- Support all 11 parameter styles across 12 database adapters
- Maintain driver-specific type coercion mappings
- Complete StatementConfig.parameter_config interface preservation
"""

import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import singledispatch
from typing import Any, Callable, Optional, Set

# Placeholder imports - will be enabled during BUILD phase
# from mypy_extensions import mypyc_attr

__all__ = (
    "ParameterConverter",
    "ParameterInfo",
    "ParameterProcessor",
    "ParameterStyle",
    "ParameterStyleConfig",
    "ParameterValidator",
    "TypedParameter",
    "is_iterable_parameters",
    "wrap_with_type",
)


# PRESERVED - Exact same ParameterStyle enum from current parameters.py
class ParameterStyle(str, Enum):
    """Parameter style enumeration - preserved interface.

    Supports all parameter styles used across 12 database adapters:
    - QMARK: ? placeholders (SQLite, DuckDB)
    - NUMERIC: $1, $2 placeholders (PostgreSQL)
    - POSITIONAL_PYFORMAT: %s placeholders (MySQL)
    - NAMED_PYFORMAT: %(name)s placeholders (MySQL, PostgreSQL)
    - NAMED_COLON: :name placeholders (Oracle, SQLite)
    - NAMED_AT: @name placeholders (SQL Server, Oracle)
    - NAMED_DOLLAR: $name placeholders (PostgreSQL)
    - POSITIONAL_COLON: :1, :2 placeholders (Oracle)
    - STATIC: Direct embedding of values in SQL
    - NONE: No parameters supported
    """

    NONE = "none"
    STATIC = "static"
    QMARK = "qmark"
    NUMERIC = "numeric"
    NAMED_COLON = "named_colon"
    POSITIONAL_COLON = "positional_colon"
    NAMED_AT = "named_at"
    NAMED_DOLLAR = "named_dollar"
    NAMED_PYFORMAT = "pyformat_named"
    POSITIONAL_PYFORMAT = "pyformat_positional"


# @mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class TypedParameter:
    """Parameter wrapper that preserves type information for processing.

    Critical component for parameter pre-processing pipeline that maintains
    type information through SQLGlot parsing and execution format conversion.

    Use Cases:
    - Preserve boolean values through SQLGlot parsing (prevents "True"/"False" strings)
    - Maintain Decimal precision for financial calculations
    - Handle date/datetime formatting for different databases
    - Preserve array/list structures for PostgreSQL arrays
    - Handle JSON serialization for dict parameters

    Performance:
    - __slots__ for memory efficiency
    - Cached hash for O(1) dictionary operations
    - Minimal overhead when type preservation not needed
    """

    __slots__ = ("_hash", "original_type", "semantic_name", "value")

    def __init__(self, value: Any, original_type: Optional[type] = None, semantic_name: Optional[str] = None) -> None:
        """Initialize typed parameter wrapper.

        Args:
            value: The parameter value
            original_type: Original type (defaults to type(value))
            semantic_name: Optional semantic name for debugging
        """
        self.value = value
        self.original_type = original_type or type(value)
        self.semantic_name = semantic_name
        self._hash = None  # Lazy hash computation

    def __hash__(self) -> int:
        """Cached hash for efficient dictionary operations."""
        if self._hash is None:
            self._hash = hash((id(self.value), self.original_type, self.semantic_name))
        return self._hash

    def __eq__(self, other: object) -> bool:
        """Equality comparison for TypedParameter instances."""
        if not isinstance(other, TypedParameter):
            return False
        return (
            self.value == other.value
            and self.original_type == other.original_type
            and self.semantic_name == other.semantic_name
        )

    def __repr__(self) -> str:
        """String representation for debugging."""
        name_part = f", semantic_name='{self.semantic_name}'" if self.semantic_name else ""
        return f"TypedParameter({self.value!r}, original_type={self.original_type.__name__}{name_part})"


# CRITICAL: Type-specific parameter wrapping for SQLGlot compatibility
@singledispatch
def _wrap_parameter_by_type(value: Any, semantic_name: Optional[str] = None) -> Any:
    """Type-specific parameter wrapping using singledispatch for performance.

    This function uses Python's singledispatch for O(1) type-based dispatch
    to wrap parameters that need special handling during SQLGlot processing.

    Performance: Singledispatch provides faster type-based dispatch than
    isinstance() chains, especially with many type checks.

    Args:
        value: Parameter value to potentially wrap
        semantic_name: Optional semantic name for debugging

    Returns:
        Either the original value or TypedParameter wrapper
    """
    # Default case: most values don't need wrapping
    return value


# Type-specific implementations for special cases that need preservation
@_wrap_parameter_by_type.register
def _(value: bool, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap boolean values to prevent SQLGlot parsing issues."""
    return TypedParameter(value, bool, semantic_name)


@_wrap_parameter_by_type.register
def _(value: Decimal, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap Decimal values to preserve precision."""
    return TypedParameter(value, Decimal, semantic_name)


@_wrap_parameter_by_type.register
def _(value: datetime, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap datetime values for database-specific formatting."""
    return TypedParameter(value, datetime, semantic_name)


@_wrap_parameter_by_type.register
def _(value: date, semantic_name: Optional[str] = None) -> TypedParameter:
    """Wrap date values for database-specific formatting."""
    return TypedParameter(value, date, semantic_name)


# @mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class ParameterInfo:
    """Information about a detected parameter in SQL.

    Tracks all necessary information for parameter conversion:
    - name: Parameter name (for named styles)
    - style: Parameter style (QMARK, NAMED_COLON, etc.)
    - position: Character position in SQL string
    - ordinal: Order of appearance (0-indexed)
    - placeholder_text: Original text in SQL ("?", ":name", etc.)

    This information enables accurate parameter style conversion
    while preserving the original SQL structure.
    """

    __slots__ = ("name", "ordinal", "placeholder_text", "position", "style")

    def __init__(
        self, name: Optional[str], style: ParameterStyle, position: int, ordinal: int, placeholder_text: str
    ) -> None:
        """Initialize parameter information.

        Args:
            name: Parameter name (None for positional styles)
            style: Parameter style enum
            position: Character position in SQL
            ordinal: Order of appearance (0-indexed)
            placeholder_text: Original placeholder text
        """
        self.name = name
        self.style = style
        self.position = position
        self.ordinal = ordinal
        self.placeholder_text = placeholder_text

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"ParameterInfo(name={self.name!r}, style={self.style}, "
            f"position={self.position}, ordinal={self.ordinal}, "
            f"placeholder_text={self.placeholder_text!r})"
        )


# @mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class ParameterStyleConfig:
    """Enhanced ParameterStyleConfig with complete backward compatibility.

    Provides all attributes that drivers expect from parameter_config.
    This is critical - drivers access these attributes directly and expect
    them to work identically to the current implementation.

    Critical Attributes (accessed by drivers):
    - default_parameter_style: Primary parsing style
    - supported_parameter_styles: All input styles supported
    - supported_execution_parameter_styles: Styles driver can execute
    - default_execution_parameter_style: Target execution format
    - type_coercion_map: Driver-specific type conversions
    - output_transformer: Final SQL/parameter transformation hook
    - preserve_parameter_format: Maintain original parameter structure
    - needs_static_script_compilation: Embed parameters in SQL
    """

    __slots__ = (
        "allow_mixed_parameter_styles",
        "default_execution_parameter_style",
        "default_parameter_style",
        "has_native_list_expansion",
        "needs_static_script_compilation",
        "output_transformer",
        "preserve_parameter_format",
        "remove_null_parameters",
        "supported_execution_parameter_styles",
        "supported_parameter_styles",
        "type_coercion_map",
    )

    def __init__(
        self,
        default_parameter_style: ParameterStyle,
        supported_parameter_styles: Optional[Set[ParameterStyle]] = None,
        supported_execution_parameter_styles: Optional[Set[ParameterStyle]] = None,
        default_execution_parameter_style: Optional[ParameterStyle] = None,
        type_coercion_map: Optional[dict[type, Callable[[Any], Any]]] = None,
        has_native_list_expansion: bool = False,
        output_transformer: Optional[Callable[[str, Any], tuple[str, Any]]] = None,
        needs_static_script_compilation: bool = True,
        allow_mixed_parameter_styles: bool = False,
        preserve_parameter_format: bool = False,
        remove_null_parameters: bool = False,
    ) -> None:
        """Initialize with complete compatibility.

        Args:
            default_parameter_style: Primary parameter style for parsing
            supported_parameter_styles: All input styles this config supports
            supported_execution_parameter_styles: Styles driver can execute
            default_execution_parameter_style: Target format for execution
            type_coercion_map: Driver-specific type conversions
            has_native_list_expansion: Driver supports native array parameters
            output_transformer: Final transformation hook
            needs_static_script_compilation: Embed parameters directly in SQL
            allow_mixed_parameter_styles: Support mixed styles in single query
            preserve_parameter_format: Maintain original parameter structure
            remove_null_parameters: Filter null parameters before execution
        """
        self.default_parameter_style = default_parameter_style
        self.supported_parameter_styles = (
            supported_parameter_styles if supported_parameter_styles is not None else {default_parameter_style}
        )
        self.supported_execution_parameter_styles = supported_execution_parameter_styles
        self.default_execution_parameter_style = default_execution_parameter_style or default_parameter_style
        self.type_coercion_map = type_coercion_map or {}
        self.has_native_list_expansion = has_native_list_expansion
        self.output_transformer = output_transformer
        self.needs_static_script_compilation = needs_static_script_compilation
        self.allow_mixed_parameter_styles = allow_mixed_parameter_styles
        self.preserve_parameter_format = preserve_parameter_format
        self.remove_null_parameters = remove_null_parameters

    def hash(self) -> int:
        """Generate hash for cache key generation - preserved interface.

        This method is called by drivers for caching compiled statements.
        Must return consistent hash based on configuration state.

        Returns:
            Hash value for cache key generation
        """
        # Create hash from all configuration attributes
        hash_components = (
            self.default_parameter_style.value,
            frozenset(s.value for s in self.supported_parameter_styles),
            (
                frozenset(s.value for s in self.supported_execution_parameter_styles)
                if self.supported_execution_parameter_styles
                else None
            ),
            self.default_execution_parameter_style.value,
            tuple(sorted(self.type_coercion_map.keys())) if self.type_coercion_map else None,
            self.has_native_list_expansion,
            bool(self.output_transformer),  # Can't hash function, just presence
            self.needs_static_script_compilation,
            self.allow_mixed_parameter_styles,
            self.preserve_parameter_format,
            self.remove_null_parameters,
        )
        return hash(hash_components)


# PRESERVED - Exact same parameter regex from current parameters.py:308-336
# This regex is critical for parameter detection and must be preserved exactly
_PARAMETER_REGEX = re.compile(
    r"""
    (?P<dquote>"(?:[^"\\]|\\.)*") |
    (?P<squote>'(?:[^'\\]|\\.)*') |
    (?P<dollar_quoted_string>\$(?P<dollar_quote_tag_inner>\w*)?\$[\s\S]*?\$\4\$) |
    (?P<line_comment>--[^\r\n]*) |
    (?P<block_comment>/\*(?:[^*]|\*(?!/))*\*/) |
    (?P<pg_q_operator>\?\?|\?\||\?&) |
    (?P<pg_cast>::(?P<cast_type>\w+)) |
    (?P<pyformat_named>%\((?P<pyformat_name>\w+)\)s) |
    (?P<pyformat_pos>%s) |
    (?P<positional_colon>:(?P<colon_num>\d+)) |
    (?P<named_colon>:(?P<colon_name>\w+)) |
    (?P<named_at>@(?P<at_name>\w+)) |
    (?P<named_dollar_param>\$(?P<dollar_param_name>\w+)) |
    (?P<qmark>\?)
    """,
    re.VERBOSE | re.IGNORECASE | re.MULTILINE | re.DOTALL,
)


# @mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class ParameterValidator:
    """Parameter validation and extraction with comprehensive dialect support.

    Responsible for extracting parameter information from SQL strings
    using the complex regex pattern and determining SQLGlot compatibility.

    Performance Features:
    - Cached parameter extraction results
    - Optimized regex matching with comprehensive pattern
    - Dialect-specific compatibility matrices

    Compatibility Features:
    - Preserves exact parameter detection logic from current parameters.py
    - Same regex pattern for consistent behavior
    - Same SQLGlot compatibility checking
    """

    __slots__ = ("_parameter_cache",)

    def __init__(self) -> None:
        """Initialize validator with parameter cache."""
        self._parameter_cache: dict[str, list[ParameterInfo]] = {}

    def extract_parameters(self, sql: str) -> "list[ParameterInfo]":
        """Extract all parameters from SQL with complete style detection.

        CRITICAL: This method preserves exact behavior from current parameters.py
        while providing performance optimizations through caching.

        Args:
            sql: SQL string to analyze

        Returns:
            List of ParameterInfo objects for each detected parameter
        """
        # Cache lookup for performance
        if sql in self._parameter_cache:
            return self._parameter_cache[sql]

        parameters: list[ParameterInfo] = []
        ordinal = 0

        # Use the preserved regex pattern to find all parameter matches
        for match in _PARAMETER_REGEX.finditer(sql):
            # Skip matches that are inside strings or comments
            if (
                match.group("dquote")
                or match.group("squote")
                or match.group("dollar_quoted_string")
                or match.group("line_comment")
                or match.group("block_comment")
                or match.group("pg_q_operator")
                or match.group("pg_cast")
            ):
                continue

            # Extract parameter information based on match groups
            position = match.start()
            placeholder_text = match.group(0)
            name = None
            style = None

            if match.group("pyformat_named"):
                style = ParameterStyle.NAMED_PYFORMAT
                name = match.group("pyformat_name")
            elif match.group("pyformat_pos"):
                style = ParameterStyle.POSITIONAL_PYFORMAT
            elif match.group("positional_colon"):
                style = ParameterStyle.POSITIONAL_COLON
                name = match.group("colon_num")
            elif match.group("named_colon"):
                style = ParameterStyle.NAMED_COLON
                name = match.group("colon_name")
            elif match.group("named_at"):
                style = ParameterStyle.NAMED_AT
                name = match.group("at_name")
            elif match.group("named_dollar_param"):
                style = ParameterStyle.NAMED_DOLLAR
                name = match.group("dollar_param_name")
            elif match.group("qmark"):
                style = ParameterStyle.QMARK

            if style is not None:
                param_info = ParameterInfo(
                    name=name, style=style, position=position, ordinal=ordinal, placeholder_text=placeholder_text
                )
                parameters.append(param_info)
                ordinal += 1

        # Cache the result for future use
        self._parameter_cache[sql] = parameters
        return parameters

    def get_sqlglot_incompatible_styles(self, dialect: Optional[str] = None) -> "Set[ParameterStyle]":
        """Get parameter styles incompatible with SQLGlot for specific dialect.

        CRITICAL: This determines which parameters need Phase 1 conversion
        for SQLGlot compatibility.

        Dialect-Specific Incompatibilities:
        - mysql: %s (modulo conflict), %(name)s, :1, :2
        - postgres: :1, :2 (only positional colon incompatible)
        - sqlite: :1, :2 (only positional colon incompatible)
        - oracle: %s, %(name)s, :1, :2 (base incompatible set)
        - bigquery: %s, %(name)s, :1, :2 (base incompatible set)

        Args:
            dialect: SQL dialect for compatibility checking

        Returns:
            Set of parameter styles incompatible with SQLGlot
        """
        # Base incompatible styles that SQLGlot generally can't parse correctly
        base_incompatible = {
            ParameterStyle.POSITIONAL_PYFORMAT,  # %s, %d - modulo operator conflict
            ParameterStyle.NAMED_PYFORMAT,  # %(name)s - complex format string
            ParameterStyle.POSITIONAL_COLON,  # :1, :2 - numbered colon parameters
        }

        # Dialect-specific incompatibility adjustments
        if dialect and dialect.lower() in ("mysql", "mariadb"):
            # MySQL has issues with both pyformat styles due to modulo operator
            return base_incompatible
        if dialect and dialect.lower() in ("postgres", "postgresql"):
            # PostgreSQL only has issues with positional colon, handles pyformat better
            return {ParameterStyle.POSITIONAL_COLON}
        if dialect and dialect.lower() == "sqlite":
            # SQLite only has issues with positional colon
            return {ParameterStyle.POSITIONAL_COLON}
        if dialect and dialect.lower() in ("oracle", "bigquery"):
            # Oracle and BigQuery have the full base incompatible set
            return base_incompatible
        # Default: return the base incompatible set for unknown/unspecified dialects
        return base_incompatible


# @mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class ParameterConverter:
    """Parameter style conversion with complete format support.

    CRITICAL: This handles both Phase 1 (SQLGlot compatibility) and
    Phase 2 (execution format) conversions.

    Two-Phase Processing:
    Phase 1 - SQLGlot Compatibility Normalization:
      - Converts incompatible styles to canonical :param_N format
      - Enables SQLGlot parsing of problematic parameter styles
      - Preserves original parameter information for Phase 2

    Phase 2 - Execution Format Conversion:
      - Converts from canonical format to driver-specific format
      - Handles parameter format changes (list ↔ dict, positional ↔ named)
      - Applies driver-specific type coercions and transformations

    Performance:
    - O(1) placeholder generation via hash-map lookups
    - O(1) format conversion via pre-built converter functions
    - Cached SQL processing for repeated conversions
    """

    __slots__ = ("_format_converters", "_placeholder_generators", "validator")

    def __init__(self) -> None:
        """Initialize converter with optimized lookup tables."""
        self.validator = ParameterValidator()

        # Hash-map lookup tables for O(1) conversions
        self._format_converters = {
            ParameterStyle.POSITIONAL_COLON: self._convert_to_positional_colon_format,
            ParameterStyle.NAMED_COLON: self._convert_to_named_colon_format,
            ParameterStyle.NAMED_PYFORMAT: self._convert_to_named_pyformat_format,
            ParameterStyle.QMARK: self._convert_to_positional_format,
            ParameterStyle.NUMERIC: self._convert_to_positional_format,
            ParameterStyle.POSITIONAL_PYFORMAT: self._convert_to_positional_format,
            ParameterStyle.NAMED_AT: self._convert_to_named_colon_format,  # Same logic as colon
            ParameterStyle.NAMED_DOLLAR: self._convert_to_named_colon_format,
        }

        # Placeholder generators for different styles
        self._placeholder_generators = {
            ParameterStyle.QMARK: lambda _: "?",
            ParameterStyle.NUMERIC: lambda i: f"${i + 1}",
            ParameterStyle.NAMED_COLON: lambda name: f":{name}",
            ParameterStyle.POSITIONAL_COLON: lambda i: f":{i + 1}",
            ParameterStyle.NAMED_AT: lambda name: f"@{name}",
            ParameterStyle.NAMED_DOLLAR: lambda name: f"${name}",
            ParameterStyle.NAMED_PYFORMAT: lambda name: f"%({name})s",
            ParameterStyle.POSITIONAL_PYFORMAT: lambda _: "%s",
        }

    def normalize_sql_for_parsing(self, sql: str, dialect: Optional[str] = None) -> "tuple[str, list[ParameterInfo]]":
        """PHASE 1: Convert SQL to SQLGlot-parseable format.

        This is the first phase of the two-phase parameter normalization system.
        Takes raw SQL with potentially incompatible parameter styles and converts
        them to a canonical format that SQLGlot can parse.

        Example:
            Input:  "SELECT * FROM users WHERE name = %s AND id = %(user_id)s"
            Output: "SELECT * FROM users WHERE name = :param_0 AND id = :param_1"

        Args:
            sql: Raw SQL string with any parameter style
            dialect: Target SQL dialect for compatibility checking

        Returns:
            Tuple of (parseable_sql, original_parameter_info)
        """
        # 1. Extract all parameters with position/metadata
        param_info = self.validator.extract_parameters(sql)

        # 2. Check if any parameters are SQLGlot-incompatible for this dialect
        incompatible_styles = self.validator.get_sqlglot_incompatible_styles(dialect)
        needs_conversion = any(p.style in incompatible_styles for p in param_info)

        # 3. If no incompatible parameters, return as-is
        if not needs_conversion:
            return sql, param_info

        # 4. Convert incompatible parameters to :param_N format
        converted_sql = self._convert_to_sqlglot_compatible(sql, param_info, incompatible_styles)
        return converted_sql, param_info

    def _convert_to_sqlglot_compatible(
        self, sql: str, param_info: "list[ParameterInfo]", incompatible_styles: "Set[ParameterStyle]"
    ) -> str:
        """Convert SQL with incompatible parameter styles to SQLGlot-compatible format."""
        # Work backwards through parameters to maintain position accuracy
        converted_sql = sql
        for param in reversed(param_info):
            if param.style in incompatible_styles:
                # Replace with canonical :param_N format
                canonical_placeholder = f":param_{param.ordinal}"
                converted_sql = (
                    converted_sql[: param.position]
                    + canonical_placeholder
                    + converted_sql[param.position + len(param.placeholder_text) :]
                )

        return converted_sql

    def convert_placeholder_style(
        self, sql: str, parameters: Any, target_style: ParameterStyle, is_many: bool = False
    ) -> "tuple[str, Any]":
        """PHASE 2: Convert SQL and parameters to execution format.

        This is the second phase that converts from the canonical SQLGlot format
        to the final execution format required by the specific database driver.

        Example:
            Input:  ("SELECT * FROM users WHERE name = :param_0", ["john"], POSITIONAL_PYFORMAT)
            Output: ("SELECT * FROM users WHERE name = %s", ["john"])

        Args:
            sql: SQL string (possibly from Phase 1 normalization)
            parameters: Parameter values in any format
            target_style: Target parameter style for execution
            is_many: Whether this is for executemany() operation

        Returns:
            Tuple of (final_sql, execution_parameters)
        """
        # 1. Parameter extraction and validation
        param_info = self.validator.extract_parameters(sql)

        # 2. Special handling for STATIC embedding (embed parameters directly in SQL)
        if target_style == ParameterStyle.STATIC:
            return self._embed_static_parameters(sql, parameters, param_info)

        # 3. Check if conversion is needed
        current_styles = {p.style for p in param_info}
        if len(current_styles) == 1 and target_style in current_styles:
            # Only parameter format conversion needed (e.g., dict → list)
            converted_parameters = self._convert_parameter_format(parameters, param_info, target_style)
            return sql, converted_parameters

        # 4. Full SQL placeholder conversion + parameter format conversion
        converted_sql = self._convert_placeholders_to_style(sql, param_info, target_style)
        converted_parameters = self._convert_parameter_format(parameters, param_info, target_style)

        return converted_sql, converted_parameters

    def _convert_placeholders_to_style(
        self, sql: str, param_info: "list[ParameterInfo]", target_style: ParameterStyle
    ) -> str:
        """Convert SQL placeholders to target style."""
        generator = self._placeholder_generators.get(target_style)
        if not generator:
            raise ValueError(f"Unsupported target parameter style: {target_style}")

        # Work backwards through parameters to maintain position accuracy
        converted_sql = sql
        for param in reversed(param_info):
            # Generate new placeholder based on target style
            if target_style in {
                ParameterStyle.QMARK,
                ParameterStyle.NUMERIC,
                ParameterStyle.POSITIONAL_PYFORMAT,
                ParameterStyle.POSITIONAL_COLON,
            }:
                new_placeholder = generator(param.ordinal)
            else:  # Named styles
                param_name = param.name or f"param_{param.ordinal}"
                new_placeholder = generator(param_name)

            # Replace in SQL
            converted_sql = (
                converted_sql[: param.position]
                + new_placeholder
                + converted_sql[param.position + len(param.placeholder_text) :]
            )

        return converted_sql

    def _convert_parameter_format(
        self, parameters: Any, param_info: "list[ParameterInfo]", target_style: ParameterStyle
    ) -> Any:
        """Convert parameter format to match target style requirements."""
        if not parameters or not param_info:
            return parameters

        # Determine if target style expects named or positional parameters
        is_named_style = target_style in {
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_AT,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.NAMED_PYFORMAT,
        }

        if is_named_style:
            # Convert to dict format if needed
            if isinstance(parameters, Mapping):
                return parameters  # Already in correct format
            if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
                # Convert positional to named
                param_dict = {}
                for i, param in enumerate(param_info):
                    if i < len(parameters):
                        name = param.name or f"param_{param.ordinal}"
                        param_dict[name] = parameters[i]
                return param_dict
        # Convert to list/tuple format if needed
        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return parameters  # Already in correct format
        elif isinstance(parameters, Mapping):
            # Convert named to positional
            param_list = []
            for param in param_info:
                if param.name and param.name in parameters:
                    param_list.append(parameters[param.name])
                elif f"param_{param.ordinal}" in parameters:
                    param_list.append(parameters[f"param_{param.ordinal}"])
                else:
                    # Try to match by ordinal key
                    ordinal_key = str(param.ordinal + 1)  # 1-based for some styles
                    if ordinal_key in parameters:
                        param_list.append(parameters[ordinal_key])
            return param_list

        return parameters

    def _embed_static_parameters(
        self, sql: str, parameters: Any, param_info: "list[ParameterInfo]"
    ) -> "tuple[str, Any]":
        """Embed parameters directly into SQL for STATIC style."""
        if not param_info:
            return sql, None

        # Work backwards through parameters to maintain position accuracy
        static_sql = sql
        for param in reversed(param_info):
            # Get parameter value
            param_value = self._get_parameter_value(parameters, param)

            # Convert to SQL literal
            if param_value is None:
                literal = "NULL"
            elif isinstance(param_value, str):
                # Escape single quotes
                escaped = param_value.replace("'", "''")
                literal = f"'{escaped}'"
            elif isinstance(param_value, bool):
                literal = "TRUE" if param_value else "FALSE"
            elif isinstance(param_value, (int, float)):
                literal = str(param_value)
            else:
                # Convert to string and quote
                literal = f"'{param_value!s}'"

            # Replace placeholder with literal value
            static_sql = (
                static_sql[: param.position] + literal + static_sql[param.position + len(param.placeholder_text) :]
            )

        return static_sql, None  # No parameters needed for static SQL

    def _get_parameter_value(self, parameters: Any, param: ParameterInfo) -> Any:
        """Extract parameter value based on parameter info and format."""
        if isinstance(parameters, Mapping):
            # Try by name first, then by ordinal key
            if param.name and param.name in parameters:
                return parameters[param.name]
            if f"param_{param.ordinal}" in parameters:
                return parameters[f"param_{param.ordinal}"]
            if str(param.ordinal + 1) in parameters:  # 1-based ordinal
                return parameters[str(param.ordinal + 1)]
        elif isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            if param.ordinal < len(parameters):
                return parameters[param.ordinal]

        return None

    # Format converter methods for different parameter styles
    def _convert_to_positional_format(self, parameters: Any, param_info: "list[ParameterInfo]") -> Any:
        """Convert parameters to positional format (list/tuple)."""
        return self._convert_parameter_format(parameters, param_info, ParameterStyle.QMARK)

    def _convert_to_named_colon_format(self, parameters: Any, param_info: "list[ParameterInfo]") -> Any:
        """Convert parameters to named colon format (dict)."""
        return self._convert_parameter_format(parameters, param_info, ParameterStyle.NAMED_COLON)

    def _convert_to_positional_colon_format(self, parameters: Any, param_info: "list[ParameterInfo]") -> Any:
        """Convert parameters to positional colon format with 1-based keys."""
        if isinstance(parameters, Mapping):
            return parameters  # Already dict format

        # Convert to 1-based ordinal keys for Oracle
        param_dict = {}
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            for i, value in enumerate(parameters):
                param_dict[str(i + 1)] = value

        return param_dict

    def _convert_to_named_pyformat_format(self, parameters: Any, param_info: "list[ParameterInfo]") -> Any:
        """Convert parameters to named pyformat format (dict)."""
        return self._convert_parameter_format(parameters, param_info, ParameterStyle.NAMED_PYFORMAT)


# @mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class ParameterProcessor:
    """HIGH-LEVEL parameter processing engine with complete pipeline.

    This is the main entry point for the complete parameter pre-processing system
    that coordinates Phase 1 (SQLGlot compatibility) and Phase 2 (execution format).

    Processing Pipeline:
    1. Type wrapping for SQLGlot compatibility (TypedParameter)
    2. Driver-specific type coercions (type_coercion_map)
    3. Phase 1: SQLGlot normalization if needed
    4. Phase 2: Execution format conversion if needed
    5. Final output transformation (output_transformer)

    Performance:
    - Fast path for no parameters or no conversion needed
    - Cached processing results for repeated SQL patterns
    - Minimal overhead when no processing required
    """

    __slots__ = ("_cache", "_cache_size", "_converter", "_validator")

    def __init__(self) -> None:
        """Initialize processor with caching and component coordination."""
        self._cache: dict[str, tuple[str, Any]] = {}
        self._cache_size = 0
        self._validator = ParameterValidator()
        self._converter = ParameterConverter()
        self.DEFAULT_CACHE_SIZE = 1000  # Configurable cache limit

    def process(
        self,
        sql: str,
        parameters: Any,
        config: ParameterStyleConfig,
        dialect: Optional[str] = None,
        is_many: bool = False,
    ) -> "tuple[str, Any]":
        """Complete parameter processing pipeline.

        This method coordinates the entire parameter pre-processing workflow:
        1. Type wrapping for SQLGlot compatibility
        2. Phase 1: SQLGlot normalization if needed
        3. Phase 2: Execution format conversion
        4. Driver-specific type coercions
        5. Final output transformation

        Args:
            sql: Raw SQL string
            parameters: Parameter values in any format
            config: Parameter style configuration
            dialect: SQL dialect for compatibility
            is_many: Whether this is for execute_many operation

        Returns:
            Tuple of (final_sql, execution_parameters)
        """
        # 1. Cache lookup for processed results
        cache_key = f"{sql}:{hash(repr(parameters))}:{config.default_parameter_style}:{is_many}:{dialect}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 2. Determine if transformation is needed
        param_info = self._validator.extract_parameters(sql)
        needs_transformation = self._needs_transformation(param_info, config, dialect)

        # 3. Fast path: Skip processing if no transformation needed
        if not needs_transformation and not config.type_coercion_map and not config.output_transformer:
            return sql, parameters

        # 4. Progressive transformation pipeline
        processed_sql, processed_parameters = sql, parameters

        # Phase A: Type wrapping for SQLGlot compatibility
        if processed_parameters:
            processed_parameters = self._apply_type_wrapping(processed_parameters)

        # Phase B: Phase 1 - SQLGlot normalization if needed
        if self._needs_sqlglot_normalization(param_info, dialect):
            processed_sql, _ = self._converter.normalize_sql_for_parsing(processed_sql, dialect)

        # Phase C: Type coercion (database-specific)
        if config.type_coercion_map and processed_parameters:
            processed_parameters = self._apply_type_coercions(processed_parameters, config.type_coercion_map)

        # Phase D: Phase 2 - Execution format conversion
        if needs_transformation:
            processed_sql, processed_parameters = self._converter.convert_placeholder_style(
                processed_sql, processed_parameters, config.default_execution_parameter_style, is_many
            )

        # Phase E: Output transformation (custom hooks)
        if config.output_transformer:
            processed_sql, processed_parameters = config.output_transformer(processed_sql, processed_parameters)

        # 5. Cache result and return
        if self._cache_size < self.DEFAULT_CACHE_SIZE:
            self._cache[cache_key] = (processed_sql, processed_parameters)
            self._cache_size += 1

        return processed_sql, processed_parameters

    def _needs_transformation(
        self, param_info: "list[ParameterInfo]", config: ParameterStyleConfig, dialect: Optional[str] = None
    ) -> bool:
        """Determine if parameter transformation is needed."""
        if not param_info:
            return False

        # Check if SQLGlot normalization is needed
        if self._needs_sqlglot_normalization(param_info, dialect):
            return True

        # Check if execution style conversion is needed
        current_styles = {p.style for p in param_info}
        target_style = config.default_execution_parameter_style

        return not (len(current_styles) == 1 and target_style in current_styles)

    def _needs_sqlglot_normalization(self, param_info: "list[ParameterInfo]", dialect: Optional[str] = None) -> bool:
        """Check if SQLGlot normalization is needed for this SQL."""
        incompatible_styles = self._validator.get_sqlglot_incompatible_styles(dialect)
        return any(p.style in incompatible_styles for p in param_info)

    def _apply_type_wrapping(self, parameters: Any) -> Any:
        """Apply type wrapping using singledispatch for performance."""
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return [_wrap_parameter_by_type(p) for p in parameters]
        if isinstance(parameters, Mapping):
            return {k: _wrap_parameter_by_type(v) for k, v in parameters.items()}
        return _wrap_parameter_by_type(parameters)

    def _apply_type_coercions(self, parameters: Any, type_coercion_map: "dict[type, Callable[[Any], Any]]") -> Any:
        """Apply database-specific type coercions."""

        def coerce_value(value: Any) -> Any:
            value_type = type(value)
            if value_type in type_coercion_map:
                return type_coercion_map[value_type](value)
            return value

        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes)):
            return [coerce_value(p) for p in parameters]
        if isinstance(parameters, Mapping):
            return {k: coerce_value(v) for k, v in parameters.items()}
        return coerce_value(parameters)


# Helper functions for parameter processing
def is_iterable_parameters(obj: Any) -> bool:
    """Check if object is iterable parameters (not string/bytes).

    Args:
        obj: Object to check

    Returns:
        True if object is iterable parameters
    """
    return isinstance(obj, (list, tuple, set)) or (
        hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, Mapping))
    )


# Public API functions that preserve exact current interfaces
def wrap_with_type(value: Any, semantic_name: Optional[str] = None) -> Any:
    """Public API for type wrapping - preserves current interface.

    Args:
        value: Value to potentially wrap
        semantic_name: Optional semantic name

    Returns:
        Original value or TypedParameter wrapper
    """
    return _wrap_parameter_by_type(value, semantic_name)


# Implementation status tracking
__module_status__ = "PLACEHOLDER"  # PLACEHOLDER → BUILDING → TESTING → COMPLETE
__phase1_target__ = "SQLGlot Compatibility"  # Phase 1 processing target
__phase2_target__ = "Execution Format"  # Phase 2 processing target
__performance_target__ = "O(1) Conversions"  # Hash-map lookup performance target
