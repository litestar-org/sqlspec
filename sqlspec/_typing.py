# ruff: noqa: RUF100, PLR0913, A002, DOC201, PLR6301, PLR0917, ARG004, ARG002, ARG001
"""Private implementation for SQLSpec typing and optional dependency shims.

Public consumers should import from :mod:`sqlspec.typing`. This module is kept
private because it centralizes optional dependency fallbacks, compatibility
aliases, and mypyc-excluded type boundaries for package internals.
"""

import enum
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, Protocol, cast, runtime_checkable

from typing_extensions import Self, TypeVar, dataclass_transform

from sqlspec.utils.module_loader import (
    dependency_flag,
    import_optional,
    import_optional_attr,
    module_available,
    resolve_optional_attr,
)

if TYPE_CHECKING:
    from attrs import AttrsInstance
    from attrs import asdict as attrs_asdict
    from attrs import define as attrs_define
    from attrs import field as attrs_field
    from attrs import fields as attrs_fields
    from attrs import has as attrs_has
    from cattrs import structure as cattrs_structure
    from cattrs import unstructure as cattrs_unstructure
    from litestar.dto.data_structures import DTOData
    from numpy import ndarray as NumpyArray  # noqa: N812
    from opentelemetry import trace
    from opentelemetry.trace import Span, Status, StatusCode, Tracer
    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame
    from prometheus_client import Counter, Gauge, Histogram
    from pyarrow import RecordBatch as ArrowRecordBatch
    from pyarrow import RecordBatchReader as ArrowRecordBatchReader
    from pyarrow import Schema as ArrowSchema
    from pyarrow import Table as ArrowTable
    from pydantic import BaseModel, FailFast, TypeAdapter

__all__ = (
    "ALLOYDB_CONNECTOR_INSTALLED",
    "ATTRS_INSTALLED",
    "CATTRS_INSTALLED",
    "CLOUD_SQL_CONNECTOR_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
    "NANOID_INSTALLED",
    "NUMPY_INSTALLED",
    "OBSTORE_INSTALLED",
    "OPENTELEMETRY_INSTALLED",
    "ORJSON_INSTALLED",
    "PANDAS_INSTALLED",
    "PGVECTOR_INSTALLED",
    "POLARS_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "UNSET",
    "UNSET_STUB",
    "UUID_UTILS_INSTALLED",
    "ArrowRecordBatch",
    "ArrowRecordBatchReader",
    "ArrowRecordBatchReaderProtocol",
    "ArrowRecordBatchResult",
    "ArrowSchema",
    "ArrowSchemaProtocol",
    "ArrowTable",
    "ArrowTableResult",
    "AttrsInstance",
    "AttrsInstanceStub",
    "BaseModel",
    "BaseModelStub",
    "Counter",
    "DTOData",
    "DTODataStub",
    "DataclassProtocol",
    "Empty",
    "EmptyEnum",
    "EmptyType",
    "FailFast",
    "FailFastStub",
    "Gauge",
    "Histogram",
    "MsgspecValidationError",
    "NumpyArray",
    "NumpyArrayStub",
    "PandasDataFrame",
    "PandasDataFrameProtocol",
    "PolarsDataFrame",
    "PolarsDataFrameProtocol",
    "Span",
    "Status",
    "StatusCode",
    "Struct",
    "StructStub",
    "T",
    "T_co",
    "Tracer",
    "TypeAdapter",
    "TypeAdapterStub",
    "UnsetType",
    "UnsetTypeStub",
    "attrs_asdict",
    "attrs_asdict_stub",
    "attrs_define",
    "attrs_define_stub",
    "attrs_field",
    "attrs_field_stub",
    "attrs_fields",
    "attrs_fields_stub",
    "attrs_has",
    "attrs_has_stub",
    "cattrs_structure",
    "cattrs_unstructure",
    "convert",
    "convert_stub",
    "import_optional",
    "import_optional_attr",
    "module_available",
    "msgspec_fields",
    "msgspec_fields_stub",
    "trace",
)


@runtime_checkable
class DataclassProtocol(Protocol):
    """Protocol for instance checking dataclasses."""

    __dataclass_fields__: "ClassVar[dict[str, Any]]"


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

# Always define stub types for type checking


class BaseModelStub:
    """Placeholder implementation."""

    model_fields: ClassVar[dict[str, Any]] = {}
    __slots__ = ("__dict__", "__pydantic_extra__", "__pydantic_fields_set__", "__pydantic_private__")

    def __init__(self, **data: Any) -> None:
        for key, value in data.items():
            setattr(self, key, value)

    def model_dump(  # noqa: PLR0913
        self,
        /,
        *,
        include: "Any | None" = None,  # noqa: ARG002
        exclude: "Any | None" = None,  # noqa: ARG002
        context: "Any | None" = None,  # noqa: ARG002
        by_alias: bool = False,  # noqa: ARG002
        exclude_unset: bool = False,  # noqa: ARG002
        exclude_defaults: bool = False,  # noqa: ARG002
        exclude_none: bool = False,  # noqa: ARG002
        round_trip: bool = False,  # noqa: ARG002
        warnings: "bool | Literal['none', 'warn', 'error']" = True,  # noqa: ARG002
        serialize_as_any: bool = False,  # noqa: ARG002
    ) -> "dict[str, Any]":
        """Placeholder implementation."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}

    def model_dump_json(  # noqa: PLR0913
        self,
        /,
        *,
        include: "Any | None" = None,  # noqa: ARG002
        exclude: "Any | None" = None,  # noqa: ARG002
        context: "Any | None" = None,  # noqa: ARG002
        by_alias: bool = False,  # noqa: ARG002
        exclude_unset: bool = False,  # noqa: ARG002
        exclude_defaults: bool = False,  # noqa: ARG002
        exclude_none: bool = False,  # noqa: ARG002
        round_trip: bool = False,  # noqa: ARG002
        warnings: "bool | Literal['none', 'warn', 'error']" = True,  # noqa: ARG002
        serialize_as_any: bool = False,  # noqa: ARG002
    ) -> str:
        """Placeholder implementation."""
        return "{}"


class TypeAdapterStub:
    """Placeholder implementation."""

    def __init__(
        self,
        type: Any,  # noqa: A002
        *,
        config: "Any | None" = None,  # noqa: ARG002
        _parent_depth: int = 2,  # noqa: ARG002
        module: "str | None" = None,  # noqa: ARG002
    ) -> None:
        """Initialize."""
        self._type = type

    def validate_python(  # noqa: PLR0913
        self,
        object: Any,
        /,
        *,
        strict: "bool | None" = None,  # noqa: ARG002
        from_attributes: "bool | None" = None,  # noqa: ARG002
        context: "dict[str, Any] | None" = None,  # noqa: ARG002
        experimental_allow_partial: "bool | Literal['off', 'on', 'trailing-strings']" = False,  # noqa: ARG002
    ) -> Any:
        """Validate Python object."""
        return object


@dataclass
class FailFastStub:
    """Placeholder implementation for FailFast."""

    fail_fast: bool = True


# Always define stub types for msgspec


@dataclass_transform()
class StructStub:
    """Placeholder implementation."""

    __struct_fields__: ClassVar[tuple[str, ...]] = ()
    __slots__ = ()

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def convert_stub(  # noqa: PLR0913
    obj: Any,  # noqa: ARG001
    type: Any,  # noqa: A002,ARG001
    *,
    strict: bool = True,  # noqa: ARG001
    from_attributes: bool = False,  # noqa: ARG001
    dec_hook: "Any | None" = None,  # noqa: ARG001
    builtin_types: "Any | None" = None,  # noqa: ARG001
    str_keys: bool = False,  # noqa: ARG001
) -> Any:
    """Placeholder implementation."""
    return {}


def msgspec_fields_stub(type_: Any, /) -> "tuple[Any, ...]":  # noqa: ARG001
    """Placeholder implementation."""
    return ()


class UnsetTypeStub(enum.Enum):
    UNSET = "UNSET"


UNSET_STUB = UnsetTypeStub.UNSET

# Try to import real implementations at runtime
try:
    from msgspec import UNSET as _REAL_UNSET
    from msgspec import Struct as _RealStruct
    from msgspec import UnsetType as _RealUnsetType
    from msgspec import ValidationError as _RealMsgspecValidationError
    from msgspec import convert as _real_convert
    from msgspec.structs import fields as _real_msgspec_fields

    MsgspecValidationError: type[Exception] = _RealMsgspecValidationError
    Struct = _RealStruct
    UnsetType = _RealUnsetType
    UNSET = _REAL_UNSET
    convert = _real_convert
    msgspec_fields = _real_msgspec_fields
except ImportError:
    MsgspecValidationError = ValueError
    Struct = StructStub  # type: ignore[assignment,misc]
    UnsetType = UnsetTypeStub  # type: ignore[assignment,misc]
    UNSET = UNSET_STUB  # type: ignore[assignment] # pyright: ignore[reportConstantRedefinition]
    convert = convert_stub
    msgspec_fields = msgspec_fields_stub  # type: ignore[assignment]


# Always define stub type for DTOData
@runtime_checkable
class DTODataStub(Protocol[T]):
    """Placeholder implementation."""

    __slots__ = ("_backend", "_data_as_builtins")

    def __init__(self, backend: Any, data_as_builtins: Any) -> None:
        """Initialize."""

    def create_instance(self, **kwargs: Any) -> T:
        return cast("T", kwargs)

    def update_instance(self, instance: T, **kwargs: Any) -> T:
        """Update instance."""
        return cast("T", kwargs)

    def as_builtins(self) -> Any:
        """Convert to builtins."""
        return {}


# Always define stub types for attrs
@dataclass_transform()
class AttrsInstanceStub:
    """Placeholder Implementation for attrs classes"""

    __attrs_attrs__: ClassVar[tuple[Any, ...]] = ()
    __slots__ = ()

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


def attrs_asdict_stub(*args: Any, **kwargs: Any) -> "dict[str, Any]":  # noqa: ARG001
    """Placeholder implementation"""
    return {}


def attrs_define_stub(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
    """Placeholder implementation"""
    return _attrs_define_identity


def _attrs_define_identity(cls: Any) -> Any:
    return cls


def attrs_field_stub(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
    """Placeholder implementation"""
    return None


def attrs_fields_stub(*args: Any, **kwargs: Any) -> "tuple[Any, ...]":  # noqa: ARG001
    """Placeholder implementation"""
    return ()


def attrs_has_stub(*args: Any, **kwargs: Any) -> bool:  # noqa: ARG001
    """Placeholder implementation"""
    return False


def cattrs_unstructure_stub(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
    """Placeholder implementation"""
    return {}


def cattrs_structure_stub(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
    """Placeholder implementation"""
    return {}


class EmptyEnum(Enum):
    """A sentinel enum used as placeholder."""

    EMPTY = 0


EmptyType = Literal[EmptyEnum.EMPTY] | UnsetType
Empty: Final = EmptyEnum.EMPTY


@runtime_checkable
class ArrowTableResult(Protocol):
    """This is a typed shim for pyarrow.Table."""

    def to_batches(self, batch_size: int) -> Any:
        return None

    @property
    def num_rows(self) -> int:
        return 0

    @property
    def num_columns(self) -> int:
        return 0

    def to_pydict(self) -> dict[str, Any]:
        return {}

    def to_string(self) -> str:
        return ""

    def from_arrays(
        self,
        arrays: list[Any],
        names: "list[str] | None" = None,
        schema: "Any | None" = None,
        metadata: "Mapping[str, Any] | None" = None,
    ) -> Any:
        return None

    def from_pydict(
        self, mapping: dict[str, Any], schema: "Any | None" = None, metadata: "Mapping[str, Any] | None" = None
    ) -> Any:
        return None

    def from_batches(self, batches: Iterable[Any], schema: Any | None = None) -> Any:
        return None


@runtime_checkable
class ArrowRecordBatchResult(Protocol):
    """This is a typed shim for pyarrow.RecordBatch."""

    def num_rows(self) -> int:
        return 0

    def num_columns(self) -> int:
        return 0

    def to_pydict(self) -> dict[str, Any]:
        return {}

    def to_pandas(self) -> Any:
        return None

    def schema(self) -> Any:
        return None

    def column(self, i: int) -> Any:
        return None

    def slice(self, offset: int = 0, length: "int | None" = None) -> Any:
        return None


@runtime_checkable
class ArrowSchemaProtocol(Protocol):
    """Typed shim for pyarrow.Schema."""

    def field(self, i: int) -> Any:
        """Get field by index."""
        ...

    @property
    def names(self) -> "list[str]":
        """Get list of field names."""
        ...

    def __len__(self) -> int:
        """Get number of fields."""
        return 0


@runtime_checkable
class ArrowRecordBatchReaderProtocol(Protocol):
    """Typed shim for pyarrow.RecordBatchReader."""

    def read_all(self) -> Any:
        """Read all batches into a table."""
        ...

    def read_next_batch(self) -> Any:
        """Read next batch."""
        ...

    def __iter__(self) -> "Iterable[Any]":
        """Iterate over batches."""
        ...


_ARROW_TABLE_SHIM = ArrowTableResult
_ARROW_RECORD_BATCH_SHIM = ArrowRecordBatchResult
_ARROW_SCHEMA_SHIM = ArrowSchemaProtocol
_ARROW_RECORD_BATCH_READER_SHIM = ArrowRecordBatchReaderProtocol


@runtime_checkable
class PandasDataFrameProtocol(Protocol):
    """Typed shim for pandas.DataFrame."""

    def __len__(self) -> int:
        """Get number of rows."""
        ...

    def __getitem__(self, key: Any) -> Any:
        """Get column or row."""
        ...


@runtime_checkable
class PolarsDataFrameProtocol(Protocol):
    """Typed shim for polars.DataFrame."""

    def __len__(self) -> int:
        """Get number of rows."""
        ...

    def __getitem__(self, key: Any) -> Any:
        """Get column or row."""
        ...


_PANDAS_DATAFRAME_SHIM = PandasDataFrameProtocol


_POLARS_DATAFRAME_SHIM = PolarsDataFrameProtocol


@runtime_checkable
class NumpyArrayStub(Protocol):
    """Protocol stub for numpy.ndarray when numpy is not installed.

    Provides minimal interface for type checking and serialization support.
    """

    def tolist(self) -> "list[Any]":
        """Convert array to Python list."""
        ...


_NUMPY_ARRAY_SHIM = NumpyArrayStub


class _SpanShim:
    def set_attribute(self, key: str, value: Any) -> None:
        return None

    def record_exception(
        self,
        exception: "Exception",
        attributes: "Mapping[str, Any] | None" = None,
        timestamp: "int | None" = None,
        escaped: bool = False,
    ) -> None:
        return None

    def set_status(self, status: Any, description: "str | None" = None) -> None:
        return None

    def end(self, end_time: "int | None" = None) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        return None


_SPAN_SHIM = _SpanShim
_SPAN_SHIM.__name__ = "Span"
_SPAN_SHIM.__qualname__ = "Span"


class _TracerShim:
    def start_span(
        self,
        name: str,
        context: Any = None,
        kind: Any = None,
        attributes: Any = None,
        links: Any = None,
        start_time: Any = None,
        record_exception: bool = True,
        set_status_on_exception: bool = True,
    ) -> "_SpanShim":
        return _SPAN_SHIM()


_TRACER_SHIM = _TracerShim
_TRACER_SHIM.__name__ = "Tracer"
_TRACER_SHIM.__qualname__ = "Tracer"


class _TraceModule:
    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: "str | None" = None,
        schema_url: "str | None" = None,
        tracer_provider: Any = None,
    ) -> "_TracerShim":
        return _TRACER_SHIM()  # pragma: no cover

    def get_tracer_provider(self) -> Any:  # pragma: no cover
        return None

    TracerProvider = type(None)  # Shim for TracerProvider if needed elsewhere
    StatusCode = type(None)  # Shim for StatusCode
    Status = type(None)  # Shim for Status


_TRACE_SHIM = _TraceModule()
_STATUS_CODE_SHIM = type(None)
_STATUS_SHIM = type(None)


class _Metric:  # Base shim for metrics
    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: tuple[str, ...] = (),
        namespace: str = "",
        subsystem: str = "",
        unit: str = "",
        registry: Any = None,
        ejemplar_fn: Any = None,
        buckets: Any = None,
        **_: Any,
    ) -> None:
        return None

    def labels(self, *labelvalues: str, **labelkwargs: str) -> "_MetricInstance":
        return _MetricInstance()


class _MetricInstance:
    def inc(self, amount: float = 1) -> None:
        return None

    def dec(self, amount: float = 1) -> None:
        return None

    def set(self, value: float) -> None:
        return None

    def observe(self, amount: float) -> None:
        return None


class _CounterShim(_Metric):
    def labels(self, *labelvalues: str, **labelkwargs: str) -> "_MetricInstance":
        return _MetricInstance()  # pragma: no cover


_COUNTER_SHIM = _CounterShim
_COUNTER_SHIM.__name__ = "Counter"
_COUNTER_SHIM.__qualname__ = "Counter"


class _GaugeShim(_Metric):
    def labels(self, *labelvalues: str, **labelkwargs: str) -> "_MetricInstance":
        return _MetricInstance()  # pragma: no cover


_GAUGE_SHIM = _GaugeShim
_GAUGE_SHIM.__name__ = "Gauge"
_GAUGE_SHIM.__qualname__ = "Gauge"


class _HistogramShim(_Metric):
    def labels(self, *labelvalues: str, **labelkwargs: str) -> "_MetricInstance":
        return _MetricInstance()  # pragma: no cover


_HISTOGRAM_SHIM = _HistogramShim
_HISTOGRAM_SHIM.__name__ = "Histogram"
_HISTOGRAM_SHIM.__qualname__ = "Histogram"


ATTRS_INSTALLED = dependency_flag("attrs")
CATTRS_INSTALLED = dependency_flag("cattrs")
CLOUD_SQL_CONNECTOR_INSTALLED = dependency_flag("google.cloud.sql.connector")
FSSPEC_INSTALLED = dependency_flag("fsspec")
LITESTAR_INSTALLED = dependency_flag("litestar")
MSGSPEC_INSTALLED = dependency_flag("msgspec")
NUMPY_INSTALLED = dependency_flag("numpy")
OBSTORE_INSTALLED = dependency_flag("obstore")
OPENTELEMETRY_INSTALLED = dependency_flag("opentelemetry")
ORJSON_INSTALLED = dependency_flag("orjson")
PANDAS_INSTALLED = dependency_flag("pandas")
PGVECTOR_INSTALLED = dependency_flag("pgvector")
POLARS_INSTALLED = dependency_flag("polars")
PROMETHEUS_INSTALLED = dependency_flag("prometheus_client")
PYARROW_INSTALLED = dependency_flag("pyarrow")
PYDANTIC_INSTALLED = dependency_flag("pydantic")
ALLOYDB_CONNECTOR_INSTALLED = dependency_flag("google.cloud.alloydb.connector")
NANOID_INSTALLED = dependency_flag("fastnanoid")
UUID_UTILS_INSTALLED = dependency_flag("uuid_utils")

_BASE_MODEL_SHIM = BaseModelStub
_TYPE_ADAPTER_SHIM = TypeAdapterStub
_FAIL_FAST_SHIM = FailFastStub
_DTO_DATA_SHIM = DTODataStub
_ATTRS_INSTANCE_SHIM = AttrsInstanceStub
_ATTRS_ASDICT_SHIM = attrs_asdict_stub
_ATTRS_DEFINE_SHIM = attrs_define_stub
_ATTRS_FIELD_SHIM = attrs_field_stub
_ATTRS_FIELDS_SHIM = attrs_fields_stub
_ATTRS_HAS_SHIM = attrs_has_stub
_CATTRS_STRUCTURE_SHIM = cattrs_structure_stub
_CATTRS_STRUCTURE_SHIM.__name__ = "cattrs_structure"
_CATTRS_STRUCTURE_SHIM.__qualname__ = "cattrs_structure"
_CATTRS_UNSTRUCTURE_SHIM = cattrs_unstructure_stub
_CATTRS_UNSTRUCTURE_SHIM.__name__ = "cattrs_unstructure"
_CATTRS_UNSTRUCTURE_SHIM.__qualname__ = "cattrs_unstructure"
_LAZY_EXPORTS: "dict[str, tuple[str, str | None, Any]]" = {
    "ArrowRecordBatch": ("pyarrow", "RecordBatch", _ARROW_RECORD_BATCH_SHIM),
    "ArrowRecordBatchReader": ("pyarrow", "RecordBatchReader", _ARROW_RECORD_BATCH_READER_SHIM),
    "ArrowSchema": ("pyarrow", "Schema", _ARROW_SCHEMA_SHIM),
    "ArrowTable": ("pyarrow", "Table", _ARROW_TABLE_SHIM),
    "AttrsInstance": ("attrs", "AttrsInstance", _ATTRS_INSTANCE_SHIM),
    "BaseModel": ("pydantic", "BaseModel", _BASE_MODEL_SHIM),
    "Counter": ("prometheus_client", "Counter", _COUNTER_SHIM),
    "DTOData": ("litestar.dto.data_structures", "DTOData", _DTO_DATA_SHIM),
    "FailFast": ("pydantic", "FailFast", _FAIL_FAST_SHIM),
    "Gauge": ("prometheus_client", "Gauge", _GAUGE_SHIM),
    "Histogram": ("prometheus_client", "Histogram", _HISTOGRAM_SHIM),
    "NumpyArray": ("numpy", "ndarray", _NUMPY_ARRAY_SHIM),
    "PandasDataFrame": ("pandas", "DataFrame", _PANDAS_DATAFRAME_SHIM),
    "PolarsDataFrame": ("polars", "DataFrame", _POLARS_DATAFRAME_SHIM),
    "Span": ("opentelemetry.trace", "Span", _SPAN_SHIM),
    "Status": ("opentelemetry.trace", "Status", _STATUS_SHIM),
    "StatusCode": ("opentelemetry.trace", "StatusCode", _STATUS_CODE_SHIM),
    "Tracer": ("opentelemetry.trace", "Tracer", _TRACER_SHIM),
    "TypeAdapter": ("pydantic", "TypeAdapter", _TYPE_ADAPTER_SHIM),
    "attrs_asdict": ("attrs", "asdict", _ATTRS_ASDICT_SHIM),
    "attrs_define": ("attrs", "define", _ATTRS_DEFINE_SHIM),
    "attrs_field": ("attrs", "field", _ATTRS_FIELD_SHIM),
    "attrs_fields": ("attrs", "fields", _ATTRS_FIELDS_SHIM),
    "attrs_has": ("attrs", "has", _ATTRS_HAS_SHIM),
    "cattrs_structure": ("cattrs", "structure", _CATTRS_STRUCTURE_SHIM),
    "cattrs_unstructure": ("cattrs", "unstructure", _CATTRS_UNSTRUCTURE_SHIM),
    "trace": ("opentelemetry.trace", None, _TRACE_SHIM),
}


def __getattr__(name: str) -> Any:
    """Resolve optional dependency symbols lazily on first access."""

    try:
        module_name, attr_name, fallback = _LAZY_EXPORTS[name]
    except KeyError:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg) from None

    resolved = resolve_optional_attr(module_name, attr_name, fallback)
    globals()[name] = resolved
    return resolved


def __dir__() -> "list[str]":
    """Expose the public surface for autocomplete and ``dir()``."""

    return sorted(set(globals()) | set(__all__))
