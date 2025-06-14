# ruff: noqa: RUF100, PLR0913, A002, DOC201, PLR6301, PLR0917, ARG004
"""This is a simple wrapper around a few important classes in each library.

This is used to ensure compatibility when one or more of the libraries are installed.
"""

from collections.abc import Iterable, Mapping
from enum import Enum
from importlib.util import find_spec
from typing import Any, ClassVar, Final, Optional, Protocol, Union, cast, runtime_checkable

from typing_extensions import Literal, TypeVar, dataclass_transform


@runtime_checkable
class DataclassProtocol(Protocol):
    """Protocol for instance checking dataclasses.

    This protocol only requires the presence of `__dataclass_fields__`, which is the
    standard attribute that Python's dataclasses module adds to all dataclass instances.
    """

    __dataclass_fields__: "ClassVar[dict[str, Any]]"


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

try:
    from pydantic import (
        BaseModel,  # pyright: ignore[reportAssignmentType]
        FailFast,  # pyright: ignore[reportGeneralTypeIssues,reportAssignmentType]
        TypeAdapter,
    )

    PYDANTIC_INSTALLED = True
except ImportError:
    from dataclasses import dataclass

    class BaseModel(Protocol):  # type: ignore[no-redef]
        """Placeholder Implementation"""

        model_fields: "ClassVar[dict[str, Any]]"

        def model_dump(
            self,
            /,
            *,
            include: "Optional[Any]" = None,
            exclude: "Optional[Any]" = None,
            context: "Optional[Any]" = None,
            by_alias: bool = False,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            round_trip: bool = False,
            warnings: "Union[bool, Literal['none', 'warn', 'error']]" = True,
            serialize_as_any: bool = False,
        ) -> "dict[str, Any]":
            """Placeholder"""
            return {}

        def model_dump_json(
            self,
            /,
            *,
            include: "Optional[Any]" = None,
            exclude: "Optional[Any]" = None,
            context: "Optional[Any]" = None,
            by_alias: bool = False,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            round_trip: bool = False,
            warnings: "Union[bool, Literal['none', 'warn', 'error']]" = True,
            serialize_as_any: bool = False,
        ) -> str:
            """Placeholder"""
            return ""

    @runtime_checkable
    class TypeAdapter(Protocol[T_co]):  # type: ignore[no-redef]
        """Placeholder Implementation"""

        def __init__(
            self,
            type: Any,  # noqa: A002
            *,
            config: "Optional[Any]" = None,
            _parent_depth: int = 2,
            module: "Optional[str]" = None,
        ) -> None:
            """Init"""

        def validate_python(
            self,
            object: Any,
            /,
            *,
            strict: "Optional[bool]" = None,
            from_attributes: "Optional[bool]" = None,
            context: "Optional[dict[str, Any]]" = None,
            experimental_allow_partial: "Union[bool, Literal['off', 'on', 'trailing-strings']]" = False,
        ) -> "T_co":
            """Stub"""
            return cast("T_co", object)

    @dataclass
    class FailFast:  # type: ignore[no-redef]
        """Placeholder Implementation for FailFast"""

        fail_fast: bool = True

    PYDANTIC_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]

try:
    from msgspec import (
        UNSET,
        Struct,
        UnsetType,  # pyright: ignore[reportAssignmentType,reportGeneralTypeIssues]
        convert,
    )

    MSGSPEC_INSTALLED: bool = True
except ImportError:
    import enum
    from collections.abc import Iterable
    from typing import Callable, Optional, Union

    @dataclass_transform()
    @runtime_checkable
    class Struct(Protocol):  # type: ignore[no-redef]
        """Placeholder Implementation"""

        __struct_fields__: "ClassVar[tuple[str, ...]]"

    def convert(  # type: ignore[no-redef]
        obj: Any,
        type: "Union[Any, type[T]]",  # noqa: A002
        *,
        strict: bool = True,
        from_attributes: bool = False,
        dec_hook: "Optional[Callable[[type, Any], Any]]" = None,
        builtin_types: "Optional[Iterable[type]]" = None,
        str_keys: bool = False,
    ) -> "Union[T, Any]":
        """Placeholder implementation"""
        return {}

    class UnsetType(enum.Enum):  # type: ignore[no-redef]
        UNSET = "UNSET"

    UNSET = UnsetType.UNSET  # pyright: ignore[reportConstantRedefinition]
    MSGSPEC_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]

try:
    from litestar.dto.data_structures import DTOData  # pyright: ignore[reportUnknownVariableType]

    LITESTAR_INSTALLED = True
except ImportError:

    @runtime_checkable
    class DTOData(Protocol[T]):  # type: ignore[no-redef]
        """Placeholder implementation"""

        __slots__ = ("_backend", "_data_as_builtins")

        def __init__(self, backend: Any, data_as_builtins: Any) -> None:
            """Placeholder init"""

        def create_instance(self, **kwargs: Any) -> T:
            return cast("T", kwargs)

        def update_instance(self, instance: T, **kwargs: Any) -> T:
            """Placeholder implementation"""
            return cast("T", kwargs)

        def as_builtins(self) -> Any:
            """Placeholder implementation"""
            return {}

    LITESTAR_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


class EmptyEnum(Enum):
    """A sentinel enum used as placeholder."""

    EMPTY = 0


EmptyType = Union[Literal[EmptyEnum.EMPTY], UnsetType]
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
        names: "Optional[list[str]]" = None,
        schema: "Optional[Any]" = None,
        metadata: "Optional[Mapping[str, Any]]" = None,
    ) -> Any:
        return None

    def from_pydict(
        self, mapping: dict[str, Any], schema: "Optional[Any]" = None, metadata: "Optional[Mapping[str, Any]]" = None
    ) -> Any:
        return None

    def from_batches(self, batches: Iterable[Any], schema: Optional[Any] = None) -> Any:
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

    def slice(self, offset: int = 0, length: "Optional[int]" = None) -> Any:
        return None


try:
    from pyarrow import RecordBatch as ArrowRecordBatch
    from pyarrow import Table as ArrowTable

    PYARROW_INSTALLED = True
except ImportError:
    ArrowTable = ArrowTableResult  # type: ignore[assignment,misc]
    ArrowRecordBatch = ArrowRecordBatchResult  # type: ignore[assignment,misc]

    PYARROW_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


try:
    from opentelemetry import trace  # pyright: ignore[reportMissingImports, reportAssignmentType]
    from opentelemetry.trace import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        Span,  # pyright: ignore[reportMissingImports, reportAssignmentType]
        Status,
        StatusCode,
        Tracer,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )

    OPENTELEMETRY_INSTALLED = True
except ImportError:
    # Define shims for when opentelemetry is not installed

    class Span:  # type: ignore[no-redef]
        def set_attribute(self, key: str, value: Any) -> None:
            return None

        def record_exception(
            self,
            exception: "Exception",
            attributes: "Optional[Mapping[str, Any]]" = None,
            timestamp: "Optional[int]" = None,
            escaped: bool = False,
        ) -> None:
            return None

        def set_status(self, status: Any, description: "Optional[str]" = None) -> None:
            return None

        def end(self, end_time: "Optional[int]" = None) -> None:
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
            instrumenting_library_version: "Optional[str]" = None,
            schema_url: "Optional[str]" = None,
            tracer_provider: Any = None,
        ) -> Tracer:
            return Tracer()  # type: ignore[abstract] # pragma: no cover

        TracerProvider = type(None)  # Shim for TracerProvider if needed elsewhere
        StatusCode = type(None)  # Shim for StatusCode
        Status = type(None)  # Shim for Status

    trace = _TraceModule()  # type: ignore[assignment]
    StatusCode = trace.StatusCode  # type: ignore[misc]
    Status = trace.Status  # type: ignore[misc]
    OPENTELEMETRY_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


try:
    from prometheus_client import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        Counter,  # pyright: ignore[reportAssignmentType]
        Gauge,  # pyright: ignore[reportAssignmentType]
        Histogram,  # pyright: ignore[reportAssignmentType]
    )

    PROMETHEUS_INSTALLED = True
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

    PROMETHEUS_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


try:
    import aiosql  # pyright: ignore[reportMissingImports, reportAssignmentType]
    from aiosql.types import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        AsyncDriverAdapterProtocol as AiosqlAsyncProtocol,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )
    from aiosql.types import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        DriverAdapterProtocol as AiosqlProtocol,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )
    from aiosql.types import ParamType as AiosqlParamType  # pyright: ignore[reportMissingImports, reportAssignmentType]
    from aiosql.types import (
        SQLOperationType as AiosqlSQLOperationType,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )
    from aiosql.types import (  # pyright: ignore[reportMissingImports, reportAssignmentType]
        SyncDriverAdapterProtocol as AiosqlSyncProtocol,  # pyright: ignore[reportMissingImports, reportAssignmentType]
    )

    AIOSQL_INSTALLED = True
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
    AiosqlParamType = Union[dict[str, Any], list[Any], tuple[Any, ...], None]  # type: ignore[misc]

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
    class AiosqlProtocol(Protocol):  # type: ignore[no-redef]
        """Placeholder for aiosql DriverAdapterProtocol"""

        def process_sql(self, query_name: str, op_type: Any, sql: str) -> str: ...

    @runtime_checkable
    class AiosqlSyncProtocol(Protocol):  # type: ignore[no-redef]
        """Placeholder for aiosql SyncDriverAdapterProtocol"""

        is_aio_driver: "ClassVar[bool]"

        def process_sql(self, query_name: str, op_type: Any, sql: str) -> str: ...
        def select(
            self, conn: Any, query_name: str, sql: str, parameters: Any, record_class: "Optional[Any]" = None
        ) -> Any: ...
        def select_one(
            self, conn: Any, query_name: str, sql: str, parameters: Any, record_class: "Optional[Any]" = None
        ) -> "Optional[Any]": ...
        def select_value(self, conn: Any, query_name: str, sql: str, parameters: Any) -> "Optional[Any]": ...
        def select_cursor(self, conn: Any, query_name: str, sql: str, parameters: Any) -> Any: ...
        def insert_update_delete(self, conn: Any, query_name: str, sql: str, parameters: Any) -> int: ...
        def insert_update_delete_many(self, conn: Any, query_name: str, sql: str, parameters: Any) -> int: ...
        def insert_returning(self, conn: Any, query_name: str, sql: str, parameters: Any) -> "Optional[Any]": ...

    @runtime_checkable
    class AiosqlAsyncProtocol(Protocol):  # type: ignore[no-redef]
        """Placeholder for aiosql AsyncDriverAdapterProtocol"""

        is_aio_driver: "ClassVar[bool]"

        def process_sql(self, query_name: str, op_type: Any, sql: str) -> str: ...
        async def select(
            self, conn: Any, query_name: str, sql: str, parameters: Any, record_class: "Optional[Any]" = None
        ) -> Any: ...
        async def select_one(
            self, conn: Any, query_name: str, sql: str, parameters: Any, record_class: "Optional[Any]" = None
        ) -> "Optional[Any]": ...
        async def select_value(self, conn: Any, query_name: str, sql: str, parameters: Any) -> "Optional[Any]": ...
        async def select_cursor(self, conn: Any, query_name: str, sql: str, parameters: Any) -> Any: ...
        async def insert_update_delete(self, conn: Any, query_name: str, sql: str, parameters: Any) -> None: ...
        async def insert_update_delete_many(self, conn: Any, query_name: str, sql: str, parameters: Any) -> None: ...
        async def insert_returning(self, conn: Any, query_name: str, sql: str, parameters: Any) -> "Optional[Any]": ...

    AIOSQL_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


FSSPEC_INSTALLED = bool(find_spec("fsspec"))
OBSTORE_INSTALLED = bool(find_spec("obstore"))
PGVECTOR_INSTALLED = bool(find_spec("pgvector"))


__all__ = (
    "AIOSQL_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
    "OBSTORE_INSTALLED",
    "OPENTELEMETRY_INSTALLED",
    "PGVECTOR_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "UNSET",
    "AiosqlAsyncProtocol",
    "AiosqlParamType",
    "AiosqlProtocol",
    "AiosqlSQLOperationType",
    "AiosqlSyncProtocol",
    "ArrowRecordBatch",
    "ArrowRecordBatchResult",
    "ArrowTable",
    "ArrowTableResult",
    "BaseModel",
    "Counter",
    "DTOData",
    "DataclassProtocol",
    "Empty",
    "EmptyEnum",
    "EmptyType",
    "FailFast",
    "Gauge",
    "Histogram",
    "Span",
    "Status",
    "StatusCode",
    "Struct",
    "T",
    "T_co",
    "Tracer",
    "TypeAdapter",
    "UnsetType",
    "aiosql",
    "convert",
    "trace",
)
