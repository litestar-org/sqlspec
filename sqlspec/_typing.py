# ruff: noqa: RUF100, PLR0913, A002, DOC201, PLR6301, PLR0917, ARG004, ARG002, ARG001
"""Wrapper around library classes for compatibility when libraries are installed."""

import enum
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from importlib.util import find_spec
from typing import Any, ClassVar, Final, Literal, Protocol, cast, runtime_checkable

from typing_extensions import TypeVar, dataclass_transform


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


# Try to import real implementations at runtime
try:
    from pydantic import BaseModel as _RealBaseModel
    from pydantic import FailFast as _RealFailFast
    from pydantic import TypeAdapter as _RealTypeAdapter

    BaseModel = _RealBaseModel
    TypeAdapter = _RealTypeAdapter
    FailFast = _RealFailFast
    PYDANTIC_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:
    BaseModel = BaseModelStub  # type: ignore[assignment,misc]
    TypeAdapter = TypeAdapterStub  # type: ignore[assignment,misc]
    FailFast = FailFastStub  # type: ignore[assignment,misc]
    PYDANTIC_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]

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


class UnsetTypeStub(enum.Enum):
    UNSET = "UNSET"


UNSET_STUB = UnsetTypeStub.UNSET

# Try to import real implementations at runtime
try:
    from msgspec import UNSET as _REAL_UNSET
    from msgspec import Struct as _RealStruct
    from msgspec import UnsetType as _RealUnsetType
    from msgspec import convert as _real_convert

    Struct = _RealStruct
    UnsetType = _RealUnsetType
    UNSET = _REAL_UNSET
    convert = _real_convert
    MSGSPEC_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:
    Struct = StructStub  # type: ignore[assignment,misc]
    UnsetType = UnsetTypeStub  # type: ignore[assignment,misc]
    UNSET = UNSET_STUB  # type: ignore[assignment] # pyright: ignore[reportConstantRedefinition]
    convert = convert_stub
    MSGSPEC_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


try:
    import orjson  # noqa: F401

    ORJSON_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:
    ORJSON_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


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


# Try to import real implementation at runtime
try:
    from litestar.dto.data_structures import DTOData as _RealDTOData  # pyright: ignore[reportUnknownVariableType]

    DTOData = _RealDTOData
    LITESTAR_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:
    DTOData = DTODataStub  # type: ignore[assignment,misc]
    LITESTAR_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


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
    return lambda cls: cls  # pyright: ignore[reportUnknownVariableType,reportUnknownLambdaType]


def attrs_field_stub(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
    """Placeholder implementation"""
    return None


def attrs_fields_stub(*args: Any, **kwargs: Any) -> "tuple[Any, ...]":  # noqa: ARG001
    """Placeholder implementation"""
    return ()


def attrs_has_stub(*args: Any, **kwargs: Any) -> bool:  # noqa: ARG001
    """Placeholder implementation"""
    return False


# Try to import real implementations at runtime
try:
    from attrs import AttrsInstance as _RealAttrsInstance  # pyright: ignore
    from attrs import asdict as _real_attrs_asdict
    from attrs import define as _real_attrs_define
    from attrs import field as _real_attrs_field
    from attrs import fields as _real_attrs_fields
    from attrs import has as _real_attrs_has

    AttrsInstance = _RealAttrsInstance
    attrs_asdict = _real_attrs_asdict
    attrs_define = _real_attrs_define
    attrs_field = _real_attrs_field
    attrs_fields = _real_attrs_fields
    attrs_has = _real_attrs_has
    ATTRS_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:
    AttrsInstance = AttrsInstanceStub  # type: ignore[misc]
    attrs_asdict = attrs_asdict_stub
    attrs_define = attrs_define_stub
    attrs_field = attrs_field_stub
    attrs_fields = attrs_fields_stub
    attrs_has = attrs_has_stub  # type: ignore[assignment]
    ATTRS_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]

try:
    from cattrs import structure as cattrs_structure
    from cattrs import unstructure as cattrs_unstructure

    CATTRS_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:

    def cattrs_unstructure(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
        """Placeholder implementation"""
        return {}

    def cattrs_structure(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
        """Placeholder implementation"""
        return {}

    CATTRS_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]  # pyright: ignore[reportConstantRedefinition]


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


try:
    from pyarrow import RecordBatch as ArrowRecordBatch
    from pyarrow import Table as ArrowTable

    PYARROW_INSTALLED = True
except ImportError:
    ArrowTable = ArrowTableResult  # type: ignore[assignment,misc]
    ArrowRecordBatch = ArrowRecordBatchResult  # type: ignore[assignment,misc]

    PYARROW_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


@runtime_checkable
class NumpyArrayStub(Protocol):
    """Protocol stub for numpy.ndarray when numpy is not installed.

    Provides minimal interface for type checking and serialization support.
    """

    def tolist(self) -> "list[Any]":
        """Convert array to Python list."""
        ...


try:
    from numpy import ndarray as NumpyArray  # noqa: N812
except ImportError:
    NumpyArray = NumpyArrayStub  # type: ignore[assignment,misc]


try:
    from opentelemetry import trace  # pyright: ignore[reportMissingImports, reportAssignmentType]
    from opentelemetry.trace import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        Span,  # pyright: ignore[reportMissingImports, reportAssignmentType]
        Status,
        StatusCode,
        Tracer,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )

    OPENTELEMETRY_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:
    # Define shims for when opentelemetry is not installed

    class Span:  # type: ignore[no-redef]
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

        def __enter__(self) -> "Span":
            return self  # type: ignore[return-value]

        def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
            return None

    class Tracer:  # type: ignore[no-redef]
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
        ) -> Span:
            return Span()  # type: ignore[abstract]

    class _TraceModule:
        def get_tracer(
            self,
            instrumenting_module_name: str,
            instrumenting_library_version: "str | None" = None,
            schema_url: "str | None" = None,
            tracer_provider: Any = None,
        ) -> Tracer:
            return Tracer()  # type: ignore[abstract] # pragma: no cover

        TracerProvider = type(None)  # Shim for TracerProvider if needed elsewhere
        StatusCode = type(None)  # Shim for StatusCode
        Status = type(None)  # Shim for Status

    trace = _TraceModule()  # type: ignore[assignment]
    StatusCode = trace.StatusCode  # type: ignore[misc]
    Status = trace.Status  # type: ignore[misc]
    OPENTELEMETRY_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]  # pyright: ignore[reportConstantRedefinition]


try:
    from prometheus_client import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        Counter,  # pyright: ignore[reportAssignmentType]
        Gauge,  # pyright: ignore[reportAssignmentType]
        Histogram,  # pyright: ignore[reportAssignmentType]
    )

    PROMETHEUS_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:
    # Define shims for when prometheus_client is not installed

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

    class Counter(_Metric):  # type: ignore[no-redef]
        def labels(self, *labelvalues: str, **labelkwargs: str) -> _MetricInstance:
            return _MetricInstance()  # pragma: no cover

    class Gauge(_Metric):  # type: ignore[no-redef]
        def labels(self, *labelvalues: str, **labelkwargs: str) -> _MetricInstance:
            return _MetricInstance()  # pragma: no cover

    class Histogram(_Metric):  # type: ignore[no-redef]
        def labels(self, *labelvalues: str, **labelkwargs: str) -> _MetricInstance:
            return _MetricInstance()  # pragma: no cover

    PROMETHEUS_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]  # pyright: ignore[reportConstantRedefinition]


try:
    import aiosql  # pyright: ignore[reportMissingImports, reportAssignmentType]
    from aiosql.types import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        AsyncDriverAdapterProtocol as AiosqlAsyncProtocol,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )
    from aiosql.types import (
        SQLOperationType as AiosqlSQLOperationType,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )
    from aiosql.types import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        SyncDriverAdapterProtocol as AiosqlSyncProtocol,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )

    AIOSQL_INSTALLED = True  # pyright: ignore[reportConstantRedefinition]
except ImportError:
    # Define shims for when aiosql is not installed

    class _AiosqlShim:
        """Placeholder aiosql module"""

        @staticmethod
        def from_path(sql_path: str, driver_adapter: Any, **kwargs: Any) -> Any:
            """Placeholder from_path method"""
            return None  # pragma: no cover

        @staticmethod
        def from_str(sql_str: str, driver_adapter: Any, **kwargs: Any) -> Any:
            """Placeholder from_str method"""
            return None  # pragma: no cover

    aiosql = _AiosqlShim()  # type: ignore[assignment]

    # Placeholder types for aiosql protocols
    AiosqlParamType = dict[str, Any] | list[Any] | tuple[Any, ...] | None

    class AiosqlSQLOperationType(Enum):  # type: ignore[no-redef]
        """Enumeration of aiosql operation types."""

        INSERT_RETURNING = 0
        INSERT_UPDATE_DELETE = 1
        INSERT_UPDATE_DELETE_MANY = 2
        SCRIPT = 3
        SELECT = 4
        SELECT_ONE = 5
        SELECT_VALUE = 6

    @runtime_checkable
    class AiosqlSyncProtocol(Protocol):  # type: ignore[no-redef]
        """Placeholder for aiosql SyncDriverAdapterProtocol"""

        is_aio_driver: "ClassVar[bool]"

        def process_sql(self, query_name: str, op_type: Any, sql: str) -> str: ...
        def select(
            self, conn: Any, query_name: str, sql: str, parameters: Any, record_class: "Any | None" = None
        ) -> Any: ...
        def select_one(
            self, conn: Any, query_name: str, sql: str, parameters: Any, record_class: "Any | None" = None
        ) -> "Any | None": ...
        def select_value(self, conn: Any, query_name: str, sql: str, parameters: Any) -> "Any | None": ...
        def select_cursor(self, conn: Any, query_name: str, sql: str, parameters: Any) -> Any: ...
        def insert_update_delete(self, conn: Any, query_name: str, sql: str, parameters: Any) -> int: ...
        def insert_update_delete_many(self, conn: Any, query_name: str, sql: str, parameters: Any) -> int: ...
        def insert_returning(self, conn: Any, query_name: str, sql: str, parameters: Any) -> "Any | None": ...

    @runtime_checkable
    class AiosqlAsyncProtocol(Protocol):  # type: ignore[no-redef]
        """Placeholder for aiosql AsyncDriverAdapterProtocol"""

        is_aio_driver: "ClassVar[bool]"

        def process_sql(self, query_name: str, op_type: Any, sql: str) -> str: ...
        async def select(
            self, conn: Any, query_name: str, sql: str, parameters: Any, record_class: "Any | None" = None
        ) -> Any: ...
        async def select_one(
            self, conn: Any, query_name: str, sql: str, parameters: Any, record_class: "Any | None" = None
        ) -> "Any | None": ...
        async def select_value(self, conn: Any, query_name: str, sql: str, parameters: Any) -> "Any | None": ...
        async def select_cursor(self, conn: Any, query_name: str, sql: str, parameters: Any) -> Any: ...
        async def insert_update_delete(self, conn: Any, query_name: str, sql: str, parameters: Any) -> None: ...
        async def insert_update_delete_many(self, conn: Any, query_name: str, sql: str, parameters: Any) -> None: ...
        async def insert_returning(self, conn: Any, query_name: str, sql: str, parameters: Any) -> "Any | None": ...

    AIOSQL_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]  # pyright: ignore[reportConstantRedefinition]


FSSPEC_INSTALLED = bool(find_spec("fsspec"))
NUMPY_INSTALLED = bool(find_spec("numpy"))
OBSTORE_INSTALLED = bool(find_spec("obstore"))
PGVECTOR_INSTALLED = bool(find_spec("pgvector"))


__all__ = (
    "AIOSQL_INSTALLED",
    "ATTRS_INSTALLED",
    "CATTRS_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
    "NUMPY_INSTALLED",
    "OBSTORE_INSTALLED",
    "OPENTELEMETRY_INSTALLED",
    "ORJSON_INSTALLED",
    "PGVECTOR_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "UNSET",
    "UNSET_STUB",
    "AiosqlAsyncProtocol",
    "AiosqlParamType",
    "AiosqlSQLOperationType",
    "AiosqlSyncProtocol",
    "ArrowRecordBatch",
    "ArrowRecordBatchResult",
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
    "NumpyArray",
    "NumpyArrayStub",
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
    "aiosql",
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
    "trace",
)
