"""Core parameter data structures and utilities."""

from collections.abc import Callable, Collection, Generator, Iterable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from functools import singledispatch
from types import MappingProxyType
from typing import Any, Literal, TypeAlias

from mypy_extensions import mypyc_attr

__all__ = (
    "ConvertedParameters",
    "DriverParameterProfile",
    "NamedParameterOutput",
    "ParameterInfo",
    "ParameterMapping",
    "ParameterPayload",
    "ParameterProcessingResult",
    "ParameterProfile",
    "ParameterSequence",
    "ParameterStyle",
    "ParameterStyleConfig",
    "PositionalParameterOutput",
    "TypedParameter",
    "is_iterable_parameters",
    "wrap_with_type",
)


ParameterMapping: TypeAlias = "Mapping[str, object]"
"""Type alias for mapping-based parameter payloads."""


ParameterSequence: TypeAlias = "Sequence[object]"
"""Type alias for sequence-based parameter payloads."""


ParameterPayload: TypeAlias = "ParameterMapping | ParameterSequence | object | None"
"""Type alias for parameter payloads accepted by the processing pipeline."""


ConvertedParameters: TypeAlias = "dict[str, Any] | list[Any] | tuple[Any, ...] | None"
"""Type alias for parameters after conversion to driver-consumable format.

This type represents the concrete output of parameter conversion functions.
Unlike :data:`ParameterPayload` (which represents inputs and can include abstract
Mapping/Sequence types), :data:`ConvertedParameters` only includes concrete types
that database drivers can directly consume.

The union includes:

- ``dict[str, Any]``: Named parameters (e.g., ``{"name": "Alice", "age": 30}``)
- ``list[Any]``: Positional parameters as list (e.g., ``["Alice", 30]``)
- ``tuple[Any, ...]``: Positional parameters as tuple (e.g., ``("Alice", 30)``)
- ``None``: When parameters are statically embedded in SQL string
"""


PositionalParameterOutput: TypeAlias = "list[Any] | tuple[Any, ...]"
"""Type alias for positional-only parameter outputs.

Used when a function is known to return only positional (not named) parameters.
This is narrower than :data:`ConvertedParameters` and excludes ``dict`` and ``None``.
"""


NamedParameterOutput: TypeAlias = "dict[str, Any]"
"""Type alias for named-only parameter outputs.

Used when a function is known to return only named (not positional) parameters.
This is narrower than :data:`ConvertedParameters` and excludes ``list``, ``tuple``, and ``None``.
"""


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterStyle(str, Enum):
    """Enumeration of supported SQL parameter placeholder styles."""

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


@mypyc_attr(allow_interpreted_subclasses=False)
class TypedParameter:
    """Wrapper that preserves original parameter type information."""

    __slots__ = ("_hash", "original_type", "semantic_name", "value")

    def __init__(self, value: Any, original_type: "type | None" = None, semantic_name: "str | None" = None) -> None:
        self.value = value
        self.original_type = original_type or type(value)
        self.semantic_name = semantic_name
        self._hash: int | None = None

    def __hash__(self) -> int:
        if self._hash is None:
            value_id = id(self.value)
            self._hash = hash((value_id, self.original_type, self.semantic_name))
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TypedParameter):
            return False
        return (
            self.value == other.value
            and self.original_type == other.original_type
            and self.semantic_name == other.semantic_name
        )

    def __repr__(self) -> str:
        name_part = f", semantic_name='{self.semantic_name}'" if self.semantic_name else ""
        return f"TypedParameter({self.value!r}, original_type={self.original_type.__name__}{name_part})"


class _TupleAdapter:
    __slots__ = ("_as_list", "_serializer")

    def __init__(self, serializer: "Callable[[Any], str]", as_list: bool) -> None:
        self._serializer = serializer
        self._as_list = as_list

    def __call__(self, value: Any) -> "Any":
        if self._as_list:
            return self._serializer(list(value))
        return self._serializer(value)


@singledispatch
def _wrap_parameter_by_type(value: Any, semantic_name: "str | None" = None) -> Any:
    return value


@_wrap_parameter_by_type.register
def _(value: bool, semantic_name: "str | None" = None) -> "TypedParameter":
    return TypedParameter(value, bool, semantic_name)


@_wrap_parameter_by_type.register
def _(value: Decimal, semantic_name: "str | None" = None) -> "TypedParameter":
    return TypedParameter(value, Decimal, semantic_name)


@_wrap_parameter_by_type.register
def _(value: datetime, semantic_name: "str | None" = None) -> "TypedParameter":
    return TypedParameter(value, datetime, semantic_name)


@_wrap_parameter_by_type.register
def _(value: date, semantic_name: "str | None" = None) -> "TypedParameter":
    return TypedParameter(value, date, semantic_name)


@_wrap_parameter_by_type.register
def _(value: time, semantic_name: "str | None" = None) -> "TypedParameter":
    return TypedParameter(value, time, semantic_name)


@_wrap_parameter_by_type.register
def _(value: bytes, semantic_name: "str | None" = None) -> "TypedParameter":
    return TypedParameter(value, bytes, semantic_name)


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterInfo:
    """Metadata describing a single detected SQL parameter."""

    __slots__ = ("name", "ordinal", "placeholder_text", "position", "style")

    def __init__(
        self, name: "str | None", style: "ParameterStyle", position: int, ordinal: int, placeholder_text: str
    ) -> None:
        self.name = name
        self.style = style
        self.position = position
        self.ordinal = ordinal
        self.placeholder_text = placeholder_text

    def __repr__(self) -> str:
        return (
            "ParameterInfo("
            f"name={self.name!r}, style={self.style!r}, position={self.position}, "
            f"ordinal={self.ordinal}, placeholder_text={self.placeholder_text!r}"
            ")"
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterStyleConfig:
    """Configuration describing parameter behaviour for a statement."""

    __slots__ = (
        "allow_mixed_parameter_styles",
        "ast_transformer",
        "default_execution_parameter_style",
        "default_parameter_style",
        "has_native_list_expansion",
        "json_deserializer",
        "json_serializer",
        "needs_static_script_compilation",
        "output_transformer",
        "preserve_original_params_for_many",
        "preserve_parameter_format",
        "strict_named_parameters",
        "supported_execution_parameter_styles",
        "supported_parameter_styles",
        "type_coercion_map",
    )

    def __init__(
        self,
        default_parameter_style: "ParameterStyle",
        supported_parameter_styles: "Collection[ParameterStyle] | None" = None,
        supported_execution_parameter_styles: "Collection[ParameterStyle] | None" = None,
        default_execution_parameter_style: "ParameterStyle | None" = None,
        type_coercion_map: "Mapping[type, Callable[[Any], Any]] | None" = None,
        has_native_list_expansion: bool = False,
        needs_static_script_compilation: bool = False,
        allow_mixed_parameter_styles: bool = False,
        preserve_parameter_format: bool = True,
        preserve_original_params_for_many: bool = False,
        output_transformer: "Callable[[str, Any], tuple[str, Any]] | None" = None,
        ast_transformer: "Callable[[Any, Any, ParameterProfile], tuple[Any, Any]] | None" = None,
        json_serializer: "Callable[[Any], str] | None" = None,
        json_deserializer: "Callable[[str], Any] | None" = None,
        strict_named_parameters: bool = True,
    ) -> None:
        self.default_parameter_style = default_parameter_style
        self.supported_parameter_styles = frozenset(supported_parameter_styles or (default_parameter_style,))
        self.supported_execution_parameter_styles = (
            frozenset(supported_execution_parameter_styles) if supported_execution_parameter_styles else None
        )
        self.default_execution_parameter_style = default_execution_parameter_style or default_parameter_style
        self.type_coercion_map = dict(type_coercion_map or {})
        self.has_native_list_expansion = has_native_list_expansion
        self.output_transformer = output_transformer
        self.ast_transformer = ast_transformer
        self.needs_static_script_compilation = needs_static_script_compilation
        self.allow_mixed_parameter_styles = allow_mixed_parameter_styles
        self.preserve_parameter_format = preserve_parameter_format
        self.preserve_original_params_for_many = preserve_original_params_for_many
        self.strict_named_parameters = strict_named_parameters
        self.json_serializer = json_serializer
        self.json_deserializer = json_deserializer

    def __hash__(self) -> int:
        hash_components = (
            self.default_parameter_style.value,
            frozenset(style.value for style in self.supported_parameter_styles),
            (
                frozenset(style.value for style in self.supported_execution_parameter_styles)
                if self.supported_execution_parameter_styles is not None
                else None
            ),
            self.default_execution_parameter_style.value,
            tuple(sorted(self.type_coercion_map.keys(), key=str)) if self.type_coercion_map else None,
            self.has_native_list_expansion,
            self.preserve_original_params_for_many,
            bool(self.output_transformer),
            self.needs_static_script_compilation,
            self.allow_mixed_parameter_styles,
            self.preserve_parameter_format,
            self.strict_named_parameters,
            bool(self.ast_transformer),
            self.json_serializer,
            self.json_deserializer,
        )
        return hash(hash_components)

    def hash(self) -> int:
        """Return the hash value for caching compatibility.

        Returns:
            Hash value matching :func:`hash` output for this config.
        """

        return hash(self)

    def replace(self, **overrides: Any) -> "ParameterStyleConfig":
        data: dict[str, Any] = {
            "default_parameter_style": self.default_parameter_style,
            "supported_parameter_styles": set(self.supported_parameter_styles),
            "supported_execution_parameter_styles": (
                set(self.supported_execution_parameter_styles)
                if self.supported_execution_parameter_styles is not None
                else None
            ),
            "default_execution_parameter_style": self.default_execution_parameter_style,
            "type_coercion_map": dict(self.type_coercion_map),
            "has_native_list_expansion": self.has_native_list_expansion,
            "needs_static_script_compilation": self.needs_static_script_compilation,
            "allow_mixed_parameter_styles": self.allow_mixed_parameter_styles,
            "preserve_parameter_format": self.preserve_parameter_format,
            "preserve_original_params_for_many": self.preserve_original_params_for_many,
            "strict_named_parameters": self.strict_named_parameters,
            "output_transformer": self.output_transformer,
            "ast_transformer": self.ast_transformer,
            "json_serializer": self.json_serializer,
            "json_deserializer": self.json_deserializer,
        }
        data.update(overrides)
        return ParameterStyleConfig(**data)

    def with_json_serializers(
        self,
        serializer: "Callable[[Any], str]",
        *,
        tuple_strategy: "Literal['list', 'tuple']" = "list",
        deserializer: "Callable[[str], Any] | None" = None,
    ) -> "ParameterStyleConfig":
        """Return a copy configured with JSON serializers for complex parameters."""

        if tuple_strategy == "list":
            tuple_adapter = _TupleAdapter(serializer, True)
        elif tuple_strategy == "tuple":
            tuple_adapter = _TupleAdapter(serializer, False)
        else:
            msg = f"Unsupported tuple_strategy: {tuple_strategy}"
            raise ValueError(msg)

        updated_type_map = dict(self.type_coercion_map)
        updated_type_map[dict] = serializer
        updated_type_map[list] = serializer
        updated_type_map[tuple] = tuple_adapter

        return self.replace(
            type_coercion_map=updated_type_map,
            json_serializer=serializer,
            json_deserializer=deserializer or self.json_deserializer,
        )


@mypyc_attr(allow_interpreted_subclasses=False)
class DriverParameterProfile:
    """Immutable adapter profile describing parameter defaults."""

    __slots__ = (
        "allow_mixed_parameter_styles",
        "custom_type_coercions",
        "default_ast_transformer",
        "default_dialect",
        "default_execution_style",
        "default_output_transformer",
        "default_style",
        "extras",
        "has_native_list_expansion",
        "json_serializer_strategy",
        "name",
        "needs_static_script_compilation",
        "preserve_original_params_for_many",
        "preserve_parameter_format",
        "statement_kwargs",
        "strict_named_parameters",
        "supported_execution_styles",
        "supported_styles",
    )

    def __init__(
        self,
        name: str,
        default_style: "ParameterStyle",
        supported_styles: "Collection[ParameterStyle]",
        default_execution_style: "ParameterStyle",
        supported_execution_styles: "Collection[ParameterStyle] | None",
        has_native_list_expansion: bool,
        preserve_parameter_format: bool,
        needs_static_script_compilation: bool,
        allow_mixed_parameter_styles: bool,
        preserve_original_params_for_many: bool,
        json_serializer_strategy: "Literal['driver', 'helper', 'none']",
        custom_type_coercions: "Mapping[type, Callable[[Any], Any]] | None" = None,
        default_output_transformer: "Callable[[str, Any], tuple[str, Any]] | None" = None,
        default_ast_transformer: "Callable[[Any, Any, ParameterProfile], tuple[Any, Any]] | None" = None,
        extras: "Mapping[str, object] | None" = None,
        default_dialect: "str | None" = None,
        statement_kwargs: "Mapping[str, object] | None" = None,
        strict_named_parameters: bool = True,
    ) -> None:
        self.name = name
        self.default_style = default_style
        self.supported_styles = frozenset(supported_styles)
        self.default_execution_style = default_execution_style
        self.supported_execution_styles = (
            frozenset(supported_execution_styles) if supported_execution_styles is not None else None
        )
        self.has_native_list_expansion = has_native_list_expansion
        self.preserve_parameter_format = preserve_parameter_format
        self.needs_static_script_compilation = needs_static_script_compilation
        self.allow_mixed_parameter_styles = allow_mixed_parameter_styles
        self.preserve_original_params_for_many = preserve_original_params_for_many
        self.strict_named_parameters = strict_named_parameters
        self.json_serializer_strategy = json_serializer_strategy
        self.custom_type_coercions = (
            MappingProxyType(dict(custom_type_coercions)) if custom_type_coercions else MappingProxyType({})
        )
        self.default_output_transformer = default_output_transformer
        self.default_ast_transformer = default_ast_transformer
        self.extras = MappingProxyType(dict(extras)) if extras else MappingProxyType({})
        self.default_dialect = default_dialect
        self.statement_kwargs = MappingProxyType(dict(statement_kwargs)) if statement_kwargs else MappingProxyType({})


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterProfile:
    """Aggregate metadata describing detected parameters."""

    __slots__ = ("_parameters", "_placeholder_counts", "named_parameters", "reused_ordinals", "styles")

    def __init__(self, parameters: "Sequence[ParameterInfo] | None" = None) -> None:
        param_tuple: tuple[ParameterInfo, ...] = tuple(parameters) if parameters else ()
        self._parameters = param_tuple
        self.styles = tuple(sorted({param.style.value for param in param_tuple})) if param_tuple else ()
        placeholder_counts: dict[str, int] = {}
        reused_ordinals: list[int] = []
        named_parameters: list[str] = []

        for param in param_tuple:
            placeholder = param.placeholder_text
            current_count = placeholder_counts.get(placeholder, 0)
            placeholder_counts[placeholder] = current_count + 1
            if current_count:
                reused_ordinals.append(param.ordinal)
            if param.name is not None:
                named_parameters.append(param.name)

        self._placeholder_counts = placeholder_counts
        self.reused_ordinals = tuple(reused_ordinals)
        self.named_parameters = tuple(named_parameters)

    @classmethod
    def empty(cls) -> "ParameterProfile":
        return cls(())

    @property
    def parameters(self) -> "tuple[ParameterInfo, ...]":
        return self._parameters

    @property
    def total_count(self) -> int:
        return len(self._parameters)

    def placeholder_count(self, placeholder: str) -> int:
        return self._placeholder_counts.get(placeholder, 0)

    def is_empty(self) -> bool:
        return not self._parameters


@mypyc_attr(allow_interpreted_subclasses=False)
class ParameterProcessingResult:
    """Return container for parameter processing output."""

    __slots__ = ("parameter_profile", "parameters", "sql", "sqlglot_sql")

    def __init__(
        self, sql: str, parameters: Any, parameter_profile: "ParameterProfile", sqlglot_sql: str | None = None
    ) -> None:
        self.sql = sql
        self.parameters = parameters
        self.parameter_profile = parameter_profile
        self.sqlglot_sql = sqlglot_sql or sql

    def __iter__(self) -> "Generator[str | Any, Any, None]":
        yield self.sql
        yield self.parameters

    def __len__(self) -> int:
        return 2

    def __getitem__(self, index: int) -> Any:
        if index == 0:
            return self.sql
        if index == 1:
            return self.parameters
        msg = "ParameterProcessingResult exposes exactly two positional items"
        raise IndexError(msg)


def is_iterable_parameters(obj: Any) -> bool:
    """Return True when the object behaves like an iterable parameter payload."""

    return isinstance(obj, (list, tuple, set)) or (
        isinstance(obj, Iterable) and not isinstance(obj, (str, bytes, Mapping))
    )


def wrap_with_type(value: Any, semantic_name: "str | None" = None) -> Any:
    """Wrap value with :class:`TypedParameter` if it benefits downstream processing."""

    return _wrap_parameter_by_type(value, semantic_name)
