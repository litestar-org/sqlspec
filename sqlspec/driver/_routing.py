"""Lazy sync and async session routing with explicit driver method contracts."""

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import AsyncExitStack, ExitStack, asynccontextmanager, contextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any, Generic, Literal, Self, cast, overload
from uuid import UUID

from typing_extensions import TypeVar

from sqlspec.core import SQL, ArrowResult, SQLResult, StackResult, Statement, StatementConfig, StatementFilter
from sqlspec.core.stack import StatementStack
from sqlspec.driver._async import AsyncDriverAdapterBase
from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver._sql_helpers import DEFAULT_PRETTY
from sqlspec.driver._sql_helpers import convert_to_dialect as _convert_to_dialect
from sqlspec.driver._stream import AsyncRowStream, SyncRowStream
from sqlspec.driver._sync import SyncDataDictionaryBase, SyncDriverAdapterBase
from sqlspec.exceptions import ImproperConfigurationError, RoutingContextError, SQLSpecError, TransactionError
from sqlspec.utils.routing import RoutingContext
from sqlspec.utils.tenant import TenantContextPayload

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.builder import QueryBuilder
    from sqlspec.config import AsyncDatabaseConfig, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
    from sqlspec.data_dictionary import (
        ColumnMetadata,
        ColumnProfileMetadata,
        DDLResult,
        ForeignKeyMetadata,
        IndexMetadata,
        MetadataCapabilityProfile,
        MetadataResult,
        SystemMetadataCapability,
        SystemMetadataRequest,
        SystemMetadataResult,
        TableMetadata,
        VersionInfo,
    )
    from sqlspec.observability import ObservabilityRuntime
    from sqlspec.storage import StorageCapabilities
    from sqlspec.storage.pipeline import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.typing import ArrowReturnFormat, SchemaT, StatementParameters
    from sqlspec.utils.schema import ValueT

__all__ = ("AsyncRoutingSession", "SyncRoutingSession")

SyncDriverT = TypeVar("SyncDriverT", bound=SyncDriverAdapterBase, default=SyncDriverAdapterBase)
AsyncDriverT = TypeVar("AsyncDriverT", bound=AsyncDriverAdapterBase, default=AsyncDriverAdapterBase)


class _RoutingSessionState:
    __slots__ = (
        "_bind_group",
        "_bypass_tenant",
        "_closed",
        "_config",
        "_pinned_primary",
        "_preparer",
        "_primary_driver",
        "_registry_lifecycle",
        "_replica_bind_group",
        "_replica_causal_token",
        "_replica_driver",
        "_replica_max_staleness",
        "_statement_config",
        "_tenant",
    )

    def _initialize(
        self,
        config: Any,
        bind_group: str | None,
        statement_config: StatementConfig | None,
        tenant: str | UUID | TenantContextPayload | None,
        bypass_tenant: bool,
    ) -> None:
        if bypass_tenant and tenant is not None:
            msg = "provide_routing_session() cannot combine tenant with bypass_tenant=True"
            raise ImproperConfigurationError(msg)
        self._config = config
        self._bind_group = bind_group
        self._statement_config = statement_config or config.statement_config
        self._tenant = tenant
        self._bypass_tenant = bypass_tenant
        self._closed = False
        self._pinned_primary = False
        self._registry_lifecycle = False
        self._primary_driver: Any = None
        self._replica_driver: Any = None
        self._replica_bind_group: str | None = None
        self._replica_causal_token: str | None = None
        self._replica_max_staleness: float | None = None
        self._preparer = CommonDriverAttributesMixin(None, self._statement_config, config.driver_features)

    @property
    def pinned_to_primary(self) -> bool:
        """Return whether a write or transaction has pinned this facade to primary."""
        return self._pinned_primary

    @property
    def statement_config(self) -> StatementConfig:
        """Return the configuration used to prepare and classify statements."""
        return self._statement_config

    @property
    def driver_features(self) -> dict[str, Any]:
        """Return the primary configuration's driver features."""
        return cast("dict[str, Any]", self._config.driver_features)

    @property
    def observability(self) -> "ObservabilityRuntime":
        """Return the primary configuration's observability runtime."""
        return cast("ObservabilityRuntime", self._config.get_observability_runtime())

    def storage_capabilities(self) -> "StorageCapabilities":
        """Return primary driver storage capabilities without acquiring a connection."""
        return self._preparer.storage_capabilities()

    def _ensure_open(self) -> None:
        if self._closed:
            msg = "Routing session is closed"
            raise SQLSpecError(msg)

    def _pin_primary(self) -> None:
        self._pinned_primary = True
        RoutingContext.mark_write(self._config.bind_key)

    def in_transaction(self) -> bool:
        """Return whether the acquired primary owns a transaction."""
        primary = self._primary_driver
        return primary is not None and bool(primary._transaction_depth or primary._connection_in_transaction())

    def _primary_only(self) -> None:
        self._ensure_open()
        entry = RoutingContext.get_entry(self._config.bind_key)
        if entry is not None and entry.mode == "replica":
            msg = "Cannot execute a primary-only operation while RoutingContext.replica() is active"
            raise RoutingContextError(msg)
        self._pin_primary()

    def _prepare(
        self,
        statement: "SQL | Statement | QueryBuilder",
        parameters: tuple[Any, ...],
        statement_config: StatementConfig | None,
        kwargs: dict[str, Any],
    ) -> SQL:
        self._ensure_open()
        return self._preparer.prepare_statement(statement, parameters, statement_config=statement_config, kwargs=kwargs)

    def prepare_statement(
        self,
        statement: "Statement | QueryBuilder",
        parameters: "tuple[StatementParameters | StatementFilter, ...]" = (),
        *,
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> SQL:
        """Prepare SQL through the shared driver pipeline without a physical checkout."""
        return self._prepare(statement, parameters, statement_config, kwargs or {})

    def _choose_replica(self, read_only: bool) -> bool:
        entry = RoutingContext.get_entry(self._config.bind_key)
        if self.in_transaction():
            if entry is not None and entry.mode == "replica":
                msg = "Cannot route to a replica while a primary transaction is active"
                raise RoutingContextError(msg)
            if not read_only:
                self._pin_primary()
            return False
        if entry is not None and entry.mode == "replica":
            if not read_only:
                msg = "Cannot execute a non-read-only statement while RoutingContext.replica() is active"
                raise RoutingContextError(msg)
            return True
        if self._pinned_primary or (entry is not None and (entry.mode == "primary" or entry.pinned_by_write)):
            if not read_only:
                self._pin_primary()
            return False
        if not read_only:
            self._pin_primary()
            return False
        return bool(
            self._config.supports_read_routing
            or self._bind_group is not None
            or (entry is not None and entry.bind_group is not None)
        )

    def _is_stack_read_only(self, stack: StatementStack) -> bool:
        for operation in stack:
            if operation.method not in {"execute", "execute_arrow"}:
                return False
            kwargs = dict(operation.keyword_arguments or {})
            statement_config = kwargs.pop("statement_config", None)
            if not self._prepare(operation.statement, operation.arguments, statement_config, kwargs).is_read_only():
                return False
        return True

    def split_script_statements(
        self, script: str, statement_config: StatementConfig, strip_trailing_semicolon: bool = False
    ) -> list[str]:
        """Split a script using the same dialect-aware rules as native drivers."""
        return self._preparer.split_script_statements(script, statement_config, strip_trailing_semicolon)

    def convert_to_dialect(
        self, statement: Statement, to_dialect: "DialectType | None" = None, pretty: bool = DEFAULT_PRETTY
    ) -> str:
        """Convert SQL to a target dialect without acquiring a connection."""
        return _convert_to_dialect(statement, self.statement_config.dialect, to_dialect, pretty)

    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: "type[SchemaT]") -> "list[SchemaT]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: None = None) -> "list[dict[str, Any]]": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: "type[SchemaT]") -> "SchemaT": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: None = None) -> "dict[str, Any]": ...
    @overload
    @staticmethod
    def to_schema(data: Any, *, schema_type: "type[SchemaT]") -> Any: ...
    @overload
    @staticmethod
    def to_schema(data: Any, *, schema_type: None = None) -> Any: ...
    @staticmethod
    def to_schema(data: Any, *, schema_type: "type[Any] | None" = None) -> Any:
        """Map records to a public schema without checking out a connection."""
        return CommonDriverAttributesMixin.to_schema(data, schema_type=schema_type)


class SyncRoutingSession(_RoutingSessionState, Generic[SyncDriverT]):
    """Lazy synchronous session selecting replicas for provably safe reads."""

    __slots__ = ("_primary_read_stack", "_stack")

    def __init__(
        self,
        config: "SyncDatabaseConfig[Any, Any, SyncDriverT] | NoPoolSyncConfig[Any, SyncDriverT]",
        *,
        bind_group: str | None = None,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | TenantContextPayload | None = None,
        bypass_tenant: bool = False,
    ) -> None:
        """Configure lazy physical checkouts and optional tenant identity."""
        self._initialize(config, bind_group, statement_config, tenant, bypass_tenant)
        self._stack = ExitStack()
        self._primary_read_stack: ExitStack | None = None

    @property
    def primary_session(self) -> "SyncDriverT | None":
        """Return the acquired primary session, if one exists."""
        return cast("SyncDriverT | None", self._primary_driver)

    @property
    def replica_session(self) -> "SyncDriverT | None":
        """Return the most recently acquired replica session, if one exists."""
        return cast("SyncDriverT | None", self._replica_driver)

    @property
    def is_async(self) -> Literal[False]:
        """Return whether this facade executes asynchronously."""
        return False

    def __enter__(self) -> Self:
        self._ensure_open()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        try:
            self._stack.__exit__(exc_type, exc_val, exc_tb)
        finally:
            self._closed = True

    def close(self) -> None:
        """Release all physical checkouts and prevent further execution."""
        self.__exit__(None, None, None)

    def _release_primary_read(self) -> None:
        if self._primary_read_stack is not None:
            try:
                self._primary_read_stack.close()
            finally:
                self._primary_read_stack = None
                self._replica_driver = None

    def _acquire_primary(self) -> SyncDriverT:
        self._ensure_open()
        if self._primary_driver is None:
            self._release_primary_read()
            with RoutingContext.primary(self._config.bind_key):
                context = self._config.provide_session(
                    statement_config=self._statement_config, tenant=self._tenant, bypass_tenant=self._bypass_tenant
                )
                if self._registry_lifecycle:
                    from sqlspec.base import _RuntimeSyncContext

                    context = _RuntimeSyncContext(context, self._config.get_observability_runtime(), self._config)
                self._primary_driver = self._stack.enter_context(context)
        return cast("SyncDriverT", self._primary_driver)

    def _acquire_replica(self) -> SyncDriverT:
        self._ensure_open()
        entry = RoutingContext.get_entry(self._config.bind_key)
        bind_group = entry.bind_group if entry is not None and entry.bind_group is not None else self._bind_group
        causal_token = entry.causal_token if entry is not None else None
        max_staleness = entry.max_staleness_seconds if entry is not None else None
        if (
            self._replica_driver is None
            or bind_group != self._replica_bind_group
            or causal_token != self._replica_causal_token
            or max_staleness != self._replica_max_staleness
        ):
            context = self._config.provide_read_session(
                bind_group=bind_group,
                statement_config=self._statement_config,
                tenant=self._tenant,
                bypass_tenant=self._bypass_tenant,
            )
            selection_receipt = getattr(context, "_selection_receipt", None)
            if self._registry_lifecycle:
                from sqlspec.base import _RuntimeReadSyncContext

                context = _RuntimeReadSyncContext(context, session=True)
            self._release_primary_read()
            lease_stack = self._stack.enter_context(ExitStack())
            self._replica_driver = lease_stack.enter_context(context)
            managed_replicas = self._config.has_replicas
            if getattr(selection_receipt, "config", None) is self._config:
                features = self._replica_driver.driver_features
                if features.get("read_only_session") or features.get("enable_follower_reads"):
                    self._primary_read_stack = lease_stack
                elif managed_replicas:
                    self._primary_driver = self._replica_driver
            self._replica_bind_group = bind_group
            self._replica_causal_token = causal_token
            self._replica_max_staleness = max_staleness
        return cast("SyncDriverT", self._replica_driver)

    def _resolve_driver(
        self,
        statement: "SQL | Statement | QueryBuilder",
        parameters: tuple[Any, ...],
        statement_config: StatementConfig | None,
        kwargs: dict[str, Any],
    ) -> tuple[SyncDriverT, SQL]:
        sql = self._prepare(statement, parameters, statement_config, kwargs)
        driver = self._acquire_replica() if self._choose_replica(sql.is_read_only()) else self._acquire_primary()
        return driver, sql

    def begin(self) -> None:
        """Pin to primary and begin an explicit transaction."""
        self._primary_only()
        driver = self._acquire_primary()
        driver.begin()

    def commit(self) -> None:
        """Commit the acquired primary, or do nothing before primary checkout."""
        self._ensure_open()
        if self._primary_driver is not None:
            self._primary_driver.commit()

    def rollback(self) -> None:
        """Roll back the acquired primary without opening a new connection."""
        self._ensure_open()
        if self._primary_driver is not None:
            self._primary_driver.rollback()

    def create_savepoint(self, name: str) -> None:
        """Create a savepoint on primary and pin subsequent automatic reads."""
        self._primary_only()
        driver = self._acquire_primary()
        driver.create_savepoint(name)

    def release_savepoint(self, name: str) -> None:
        """Release a savepoint on an already acquired primary."""
        self._ensure_open()
        if self._primary_driver is None:
            msg = "No active primary transaction for savepoint operation"
            raise TransactionError(msg)
        self._primary_driver.release_savepoint(name)

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a savepoint on an already acquired primary."""
        self._ensure_open()
        if self._primary_driver is None:
            msg = "No active primary transaction for savepoint operation"
            raise TransactionError(msg)
        self._primary_driver.rollback_to_savepoint(name)

    @contextmanager
    def transaction(self) -> Iterator[Self]:
        """Own a primary transaction while preserving nested savepoint behavior.

        Yields:
            This routing facade with its primary transaction active.
        """
        self._primary_only()
        driver = self._acquire_primary()
        with driver.transaction():
            yield self

    @contextmanager
    def without_indexes(self, table: str, *, schema: str | None = None, concurrent: bool = False) -> Iterator[Any]:
        """Temporarily remove indexes using the primary's native context.

        Yields:
            The primary driver's index restoration context.
        """
        self._primary_only()
        driver = self._acquire_primary()
        with driver.without_indexes(table, schema=schema, concurrent=concurrent) as context:
            yield context

    def execute_stack(self, stack: StatementStack, *, continue_on_error: bool = False) -> tuple[StackResult, ...]:
        """Execute a stack on one connection, pinning any mixed or write stack."""
        self._ensure_open()
        driver = (
            self._acquire_replica()
            if self._choose_replica(self._is_stack_read_only(stack))
            else self._acquire_primary()
        )
        return driver.execute_stack(stack, continue_on_error=continue_on_error)

    @property
    def data_dictionary(self) -> SyncDataDictionaryBase:
        """Return the primary data dictionary for explicit metadata operations."""
        self._primary_only()
        return self._acquire_primary().data_dictionary

    def execute(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a statement with parameter handling."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.execute(sql)

    def execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute statement multiple times with different parameters."""
        self._primary_only()
        driver = self._acquire_primary()
        return driver.execute_many(statement, parameters, *filters, statement_config=statement_config, **kwargs)

    def execute_script(
        self,
        statement: "str | SQL",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a multi-statement script."""
        self._primary_only()
        driver = self._acquire_primary()
        return driver.execute_script(statement, *parameters, statement_config=statement_config, **kwargs)

    @overload
    def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT]": ...

    @overload
    def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[dict[str, Any]]": ...

    def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT] | list[dict[str, Any]]":
        """Execute a select statement and return all rows."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.select(sql, schema_type=schema_type)

    @overload
    def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT": ...

    @overload
    def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...

    def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Execute a select statement and return exactly one row."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.select_one(sql, schema_type=schema_type)

    @overload
    def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | None": ...

    @overload
    def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any] | None": ...

    def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any] | None":
        """Execute a select statement and return at most one row."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.select_one_or_none(sql, schema_type=schema_type)

    @overload
    def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT": ...

    @overload
    def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...

    def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | Any":
        """Execute a select statement and return a single scalar value."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.select_value(sql, value_type=value_type)

    @overload
    def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | None": ...

    @overload
    def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...

    def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | Any | None":
        """Execute a select statement and return a single scalar value or None."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.select_value_or_none(sql, value_type=value_type)

    @overload
    def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[SchemaT], int]": ...

    @overload
    def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...

    def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[SchemaT] | list[dict[str, Any]], int]":
        """Execute a select statement and return both the data and total count."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.select_with_total(sql, schema_type=schema_type, count_with_window=count_with_window)

    def select_to_arrow(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        return_format: "ArrowReturnFormat" = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute query and return results as Apache Arrow format."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.select_to_arrow(
            sql, return_format=return_format, native_only=native_only, batch_size=batch_size, arrow_schema=arrow_schema
        )

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "SyncRowStream[SchemaT]": ...

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "SyncRowStream[dict[str, Any]]": ...

    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "SyncRowStream[SchemaT] | SyncRowStream[dict[str, Any]]":
        """Execute a query and stream rows in chunks."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, kwargs)
        return driver.select_stream(sql, schema_type=schema_type, chunk_size=chunk_size, native_only=native_only)

    def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Stream a SELECT statement directly into storage."""
        driver, sql = self._resolve_driver(statement, parameters, statement_config, {})
        return driver.select_to_storage(
            sql, destination, partitioner=partitioner, format_hint=format_hint, telemetry=telemetry
        )

    def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into the target table."""
        self._primary_only()
        driver = self._acquire_primary()
        return driver.load_from_arrow(
            table,
            source,
            partitioner=partitioner,
            overwrite=overwrite,
            telemetry=telemetry,
            stage_table=stage_table,
            conflict_keys=conflict_keys,
            on_conflict=on_conflict,
            exclude_update_columns=exclude_update_columns,
            drop_stage_table=drop_stage_table,
            drop_indexes=drop_indexes,
            concurrent_indexes=concurrent_indexes,
            type_casts=type_casts,
        )

    def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        storage_options: "dict[str, Any] | None" = None,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        checkpoint_table: str | None = None,
        checkpoint_phase: str = "default",
        max_workers: int = 1,
        executor: Literal["thread", "process"] = "thread",
        arrow_schema: Any = None,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load artifacts from storage into the target table."""
        self._primary_only()
        driver = self._acquire_primary()
        return driver.load_from_storage(
            table,
            source,
            file_format=file_format,
            partitioner=partitioner,
            overwrite=overwrite,
            storage_options=storage_options,
            stage_table=stage_table,
            conflict_keys=conflict_keys,
            on_conflict=on_conflict,
            exclude_update_columns=exclude_update_columns,
            drop_stage_table=drop_stage_table,
            drop_indexes=drop_indexes,
            concurrent_indexes=concurrent_indexes,
            checkpoint_table=checkpoint_table,
            checkpoint_phase=checkpoint_phase,
            max_workers=max_workers,
            executor=executor,
            arrow_schema=arrow_schema,
            type_casts=type_casts,
        )

    def load_from_records(
        self,
        table: str,
        records: "Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]",
        *,
        columns: "list[str] | None" = None,
        overwrite: bool = False,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load in-memory dict or positional records into the target table."""
        self._primary_only()
        driver = self._acquire_primary()
        return driver.load_from_records(
            table,
            records,
            columns=columns,
            overwrite=overwrite,
            stage_table=stage_table,
            conflict_keys=conflict_keys,
            on_conflict=on_conflict,
            exclude_update_columns=exclude_update_columns,
            drop_stage_table=drop_stage_table,
            drop_indexes=drop_indexes,
            concurrent_indexes=concurrent_indexes,
            type_casts=type_casts,
        )

    fetch = select
    fetch_one = select_one
    fetch_one_or_none = select_one_or_none
    fetch_value = select_value
    fetch_value_or_none = select_value_or_none
    fetch_with_total = select_with_total
    fetch_to_arrow = select_to_arrow
    fetch_stream = select_stream


class AsyncRoutingSession(_RoutingSessionState, Generic[AsyncDriverT]):
    """Lazy asynchronous session selecting replicas for provably safe reads."""

    __slots__ = ("_primary_read_stack", "_stack")

    def __init__(
        self,
        config: "AsyncDatabaseConfig[Any, Any, AsyncDriverT] | NoPoolAsyncConfig[Any, AsyncDriverT]",
        *,
        bind_group: str | None = None,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | TenantContextPayload | None = None,
        bypass_tenant: bool = False,
    ) -> None:
        """Configure lazy physical checkouts and optional tenant identity."""
        self._initialize(config, bind_group, statement_config, tenant, bypass_tenant)
        self._stack = AsyncExitStack()
        self._primary_read_stack: AsyncExitStack | None = None

    @property
    def primary_session(self) -> "AsyncDriverT | None":
        """Return the acquired primary session, if one exists."""
        return cast("AsyncDriverT | None", self._primary_driver)

    @property
    def replica_session(self) -> "AsyncDriverT | None":
        """Return the most recently acquired replica session, if one exists."""
        return cast("AsyncDriverT | None", self._replica_driver)

    @property
    def is_async(self) -> Literal[True]:
        """Return whether this facade executes asynchronously."""
        return True

    async def __aenter__(self) -> Self:
        self._ensure_open()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        try:
            await self._stack.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self._closed = True

    async def aclose(self) -> None:
        """Release all physical checkouts and prevent further execution."""
        await self.__aexit__(None, None, None)

    async def _release_primary_read(self) -> None:
        if self._primary_read_stack is not None:
            try:
                await self._primary_read_stack.aclose()
            finally:
                self._primary_read_stack = None
                self._replica_driver = None

    async def _acquire_primary(self) -> AsyncDriverT:
        self._ensure_open()
        if self._primary_driver is None:
            await self._release_primary_read()
            with RoutingContext.primary(self._config.bind_key):
                context = self._config.provide_session(
                    statement_config=self._statement_config, tenant=self._tenant, bypass_tenant=self._bypass_tenant
                )
                if self._registry_lifecycle:
                    from sqlspec.base import _RuntimeAsyncContext

                    context = _RuntimeAsyncContext(context, self._config.get_observability_runtime(), self._config)
                self._primary_driver = await self._stack.enter_async_context(context)
        return cast("AsyncDriverT", self._primary_driver)

    async def _acquire_replica(self) -> AsyncDriverT:
        self._ensure_open()
        entry = RoutingContext.get_entry(self._config.bind_key)
        bind_group = entry.bind_group if entry is not None and entry.bind_group is not None else self._bind_group
        causal_token = entry.causal_token if entry is not None else None
        max_staleness = entry.max_staleness_seconds if entry is not None else None
        if (
            self._replica_driver is None
            or bind_group != self._replica_bind_group
            or causal_token != self._replica_causal_token
            or max_staleness != self._replica_max_staleness
        ):
            context = self._config.provide_read_session(
                bind_group=bind_group,
                statement_config=self._statement_config,
                tenant=self._tenant,
                bypass_tenant=self._bypass_tenant,
            )
            selection_receipt = getattr(context, "_selection_receipt", None)
            if self._registry_lifecycle:
                from sqlspec.base import _RuntimeReadAsyncContext

                context = _RuntimeReadAsyncContext(context, session=True)
            await self._release_primary_read()
            lease_stack = await self._stack.enter_async_context(AsyncExitStack())
            self._replica_driver = await lease_stack.enter_async_context(context)
            managed_replicas = self._config.has_replicas
            if getattr(selection_receipt, "config", None) is self._config:
                features = self._replica_driver.driver_features
                if features.get("read_only_session") or features.get("enable_follower_reads"):
                    self._primary_read_stack = lease_stack
                elif managed_replicas:
                    self._primary_driver = self._replica_driver
            self._replica_bind_group = bind_group
            self._replica_causal_token = causal_token
            self._replica_max_staleness = max_staleness
        return cast("AsyncDriverT", self._replica_driver)

    async def _resolve_driver(
        self,
        statement: "SQL | Statement | QueryBuilder",
        parameters: tuple[Any, ...],
        statement_config: StatementConfig | None,
        kwargs: dict[str, Any],
    ) -> tuple[AsyncDriverT, SQL]:
        sql = self._prepare(statement, parameters, statement_config, kwargs)
        driver = (
            await self._acquire_replica() if self._choose_replica(sql.is_read_only()) else await self._acquire_primary()
        )
        return driver, sql

    async def begin(self) -> None:
        """Pin to primary and begin an explicit transaction."""
        self._primary_only()
        driver = await self._acquire_primary()
        await driver.begin()

    async def commit(self) -> None:
        """Commit the acquired primary, or do nothing before primary checkout."""
        self._ensure_open()
        if self._primary_driver is not None:
            await self._primary_driver.commit()

    async def rollback(self) -> None:
        """Roll back the acquired primary without opening a new connection."""
        self._ensure_open()
        if self._primary_driver is not None:
            await self._primary_driver.rollback()

    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint on primary and pin subsequent automatic reads."""
        self._primary_only()
        driver = await self._acquire_primary()
        await driver.create_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        """Release a savepoint on an already acquired primary."""
        self._ensure_open()
        if self._primary_driver is None:
            msg = "No active primary transaction for savepoint operation"
            raise TransactionError(msg)
        await self._primary_driver.release_savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a savepoint on an already acquired primary."""
        self._ensure_open()
        if self._primary_driver is None:
            msg = "No active primary transaction for savepoint operation"
            raise TransactionError(msg)
        await self._primary_driver.rollback_to_savepoint(name)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Self]:
        """Own a primary transaction while preserving nested savepoint behavior.

        Yields:
            This routing facade with its primary transaction active.
        """
        self._primary_only()
        driver = await self._acquire_primary()
        async with driver.transaction():
            yield self

    @asynccontextmanager
    async def without_indexes(
        self, table: str, *, schema: str | None = None, concurrent: bool = False
    ) -> AsyncIterator[Any]:
        """Temporarily remove indexes using the primary's native context.

        Yields:
            The primary driver's index restoration context.
        """
        self._primary_only()
        driver = await self._acquire_primary()
        async with driver.without_indexes(table, schema=schema, concurrent=concurrent) as context:
            yield context

    async def execute_stack(self, stack: StatementStack, *, continue_on_error: bool = False) -> tuple[StackResult, ...]:
        """Execute a stack on one connection, pinning any mixed or write stack."""
        self._ensure_open()
        driver = (
            await self._acquire_replica()
            if self._choose_replica(self._is_stack_read_only(stack))
            else await self._acquire_primary()
        )
        return await driver.execute_stack(stack, continue_on_error=continue_on_error)

    @property
    def data_dictionary(self) -> "_AsyncRoutingDataDictionary[AsyncDriverT]":
        """Return a lazy dictionary facade whose awaited operations use primary."""
        return _AsyncRoutingDataDictionary(self)

    async def execute(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a statement with parameter handling."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, kwargs)
        return await driver.execute(sql)

    async def execute_many(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        parameters: "Sequence[StatementParameters]",
        *filters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute statement multiple times with different parameters."""
        self._primary_only()
        driver = await self._acquire_primary()
        return await driver.execute_many(statement, parameters, *filters, statement_config=statement_config, **kwargs)

    async def execute_script(
        self,
        statement: "str | SQL",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a multi-statement script."""
        self._primary_only()
        driver = await self._acquire_primary()
        return await driver.execute_script(statement, *parameters, statement_config=statement_config, **kwargs)

    @overload
    async def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT]": ...

    @overload
    async def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[dict[str, Any]]": ...

    async def select(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "list[SchemaT] | list[dict[str, Any]]":
        """Execute a select statement and return all rows."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, kwargs)
        return await driver.select(sql, schema_type=schema_type)

    @overload
    async def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT": ...

    @overload
    async def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...

    async def select_one(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any]":
        """Execute a select statement and return exactly one row."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, kwargs)
        return await driver.select_one(sql, schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | None": ...

    @overload
    async def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "dict[str, Any] | None": ...

    async def select_one_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "SchemaT | dict[str, Any] | None":
        """Execute a select statement and return at most one row."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, kwargs)
        return await driver.select_one_or_none(sql, schema_type=schema_type)

    @overload
    async def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT": ...

    @overload
    async def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...

    async def select_value(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | Any":
        """Execute a select statement and return a single scalar value."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, kwargs)
        return await driver.select_value(sql, value_type=value_type)

    @overload
    async def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT]",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | None": ...

    @overload
    async def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: None = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> Any: ...

    async def select_value_or_none(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        value_type: "type[ValueT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "ValueT | Any | None":
        """Execute a select statement and return a single scalar value or None."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, kwargs)
        return await driver.select_value_or_none(sql, value_type=value_type)

    @overload
    async def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[SchemaT], int]": ...

    @overload
    async def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[dict[str, Any]], int]": ...

    async def select_with_total(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        count_with_window: bool = False,
        **kwargs: Any,
    ) -> "tuple[list[SchemaT] | list[dict[str, Any]], int]":
        """Execute a select statement and return both the data and total count."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, kwargs)
        return await driver.select_with_total(sql, schema_type=schema_type, count_with_window=count_with_window)

    async def select_to_arrow(
        self,
        statement: "Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        return_format: "ArrowReturnFormat" = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any = None,
        **kwargs: Any,
    ) -> "ArrowResult":
        """Execute query and return results as Apache Arrow format (async)."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, kwargs)
        return await driver.select_to_arrow(
            sql, return_format=return_format, native_only=native_only, batch_size=batch_size, arrow_schema=arrow_schema
        )

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT]",
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[SchemaT]": ...

    @overload
    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: None = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[dict[str, Any]]": ...

    def select_stream(
        self,
        statement: "SQL | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        schema_type: "type[SchemaT] | None" = None,
        statement_config: "StatementConfig | None" = None,
        chunk_size: int = 1000,
        native_only: bool = False,
        **kwargs: Any,
    ) -> "AsyncRowStream[SchemaT] | AsyncRowStream[dict[str, Any]]":
        """Execute a query and stream rows in chunks."""
        self._ensure_open()
        if chunk_size < 1:
            msg = "chunk_size must be positive"
            raise ValueError(msg)
        source = _AsyncRoutingRowSource(self, statement, parameters, statement_config, kwargs, chunk_size, native_only)
        return AsyncRowStream(source, schema_type=schema_type)

    async def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Stream a SELECT statement directly into storage."""
        driver, sql = await self._resolve_driver(statement, parameters, statement_config, {})
        return await driver.select_to_storage(
            sql, destination, partitioner=partitioner, format_hint=format_hint, telemetry=telemetry
        )

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into the target table."""
        self._primary_only()
        driver = await self._acquire_primary()
        return await driver.load_from_arrow(
            table,
            source,
            partitioner=partitioner,
            overwrite=overwrite,
            telemetry=telemetry,
            stage_table=stage_table,
            conflict_keys=conflict_keys,
            on_conflict=on_conflict,
            exclude_update_columns=exclude_update_columns,
            drop_stage_table=drop_stage_table,
            drop_indexes=drop_indexes,
            concurrent_indexes=concurrent_indexes,
            type_casts=type_casts,
        )

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        storage_options: "dict[str, Any] | None" = None,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        checkpoint_table: str | None = None,
        checkpoint_phase: str = "default",
        max_workers: int = 1,
        executor: Literal["thread", "process"] = "thread",
        arrow_schema: Any = None,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load artifacts from storage into the target table."""
        self._primary_only()
        driver = await self._acquire_primary()
        return await driver.load_from_storage(
            table,
            source,
            file_format=file_format,
            partitioner=partitioner,
            overwrite=overwrite,
            storage_options=storage_options,
            stage_table=stage_table,
            conflict_keys=conflict_keys,
            on_conflict=on_conflict,
            exclude_update_columns=exclude_update_columns,
            drop_stage_table=drop_stage_table,
            drop_indexes=drop_indexes,
            concurrent_indexes=concurrent_indexes,
            checkpoint_table=checkpoint_table,
            checkpoint_phase=checkpoint_phase,
            max_workers=max_workers,
            executor=executor,
            arrow_schema=arrow_schema,
            type_casts=type_casts,
        )

    async def load_from_records(
        self,
        table: str,
        records: "Sequence[Mapping[str, Any]] | Sequence[Sequence[Any]]",
        *,
        columns: "list[str] | None" = None,
        overwrite: bool = False,
        stage_table: "bool | str | None" = None,
        conflict_keys: "Sequence[str] | None" = None,
        on_conflict: Literal["update", "nothing"] = "update",
        exclude_update_columns: "Sequence[str] | None" = None,
        drop_stage_table: bool = True,
        drop_indexes: bool = False,
        concurrent_indexes: bool = False,
        type_casts: "Mapping[str, str] | None" = None,
    ) -> "StorageBridgeJob":
        """Load in-memory dict or positional records into the target table."""
        self._primary_only()
        driver = await self._acquire_primary()
        return await driver.load_from_records(
            table,
            records,
            columns=columns,
            overwrite=overwrite,
            stage_table=stage_table,
            conflict_keys=conflict_keys,
            on_conflict=on_conflict,
            exclude_update_columns=exclude_update_columns,
            drop_stage_table=drop_stage_table,
            drop_indexes=drop_indexes,
            concurrent_indexes=concurrent_indexes,
            type_casts=type_casts,
        )

    fetch = select
    fetch_one = select_one
    fetch_one_or_none = select_one_or_none
    fetch_value = select_value
    fetch_value_or_none = select_value_or_none
    fetch_with_total = select_with_total
    fetch_to_arrow = select_to_arrow
    fetch_stream = select_stream


class _AsyncRoutingRowSource:
    """Acquire the selected session only when an async stream starts consuming."""

    __slots__ = (
        "_chunk_size",
        "_inner_stream",
        "_kwargs",
        "_native_only",
        "_parameters",
        "_routing_session",
        "_statement",
        "_statement_config",
    )

    def __init__(
        self,
        session: AsyncRoutingSession[Any],
        statement: "SQL | Statement | QueryBuilder",
        parameters: tuple[Any, ...],
        statement_config: StatementConfig | None,
        kwargs: dict[str, Any],
        chunk_size: int,
        native_only: bool,
    ) -> None:
        self._routing_session = session
        self._statement = statement
        self._parameters = parameters
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._chunk_size = chunk_size
        self._native_only = native_only
        self._inner_stream: AsyncRowStream[dict[str, Any]] | None = None

    async def start(self) -> None:
        driver, sql = await self._routing_session._resolve_driver(
            self._statement, self._parameters, self._statement_config, self._kwargs
        )
        inner = driver.select_stream(sql, chunk_size=self._chunk_size, native_only=self._native_only)
        self._inner_stream = inner
        await inner._source.start()

    @property
    def column_types(self) -> dict[str, str] | None:
        """Expose native stream type metadata after lazy acquisition."""
        return self._inner_stream.column_types if self._inner_stream is not None else None

    async def fetch_chunk(self) -> list[dict[str, Any]]:
        if self._inner_stream is None:
            return []
        return await self._inner_stream._source.fetch_chunk()

    async def close(self, error: bool = False) -> None:
        if self._inner_stream is not None:
            await self._inner_stream._source.close(error=error)
            self._inner_stream = None


class _AsyncRoutingDataDictionary(Generic[AsyncDriverT]):
    """Resolve primary lazily for asynchronous dictionary operations."""

    __slots__ = ("_routing_session",)

    def __init__(self, session: AsyncRoutingSession[AsyncDriverT]) -> None:
        self._routing_session = session

    async def get_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "MetadataCapabilityProfile":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_metadata_capabilities(primary, domains)

    async def get_system_metadata_capabilities(
        self, driver: Any, domains: "Sequence[str] | None" = None
    ) -> "tuple[SystemMetadataCapability, ...]":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_system_metadata_capabilities(primary, domains)

    async def get_schemas(self, driver: Any) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_schemas(primary)

    async def get_objects(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_objects(primary, schema)

    async def get_table_details(self, driver: Any, table: str, schema: "str | None" = None) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_table_details(primary, table, schema)

    async def get_constraints(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_constraints(primary, table, schema)

    async def get_views(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_views(primary, schema)

    async def get_routines(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_routines(primary, schema)

    async def get_privileges(
        self, driver: Any, object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_privileges(primary, object_name, schema)

    async def get_dependencies(
        self, driver: Any, object_name: "str | None" = None, schema: "str | None" = None
    ) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_dependencies(primary, object_name, schema)

    async def get_sequences(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_sequences(primary, schema)

    async def get_types(self, driver: Any, schema: "str | None" = None) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_types(primary, schema)

    async def get_ddl(
        self,
        driver: Any,
        object_name: str,
        schema: "str | None" = None,
        *,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "DDLResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_ddl(
            primary,
            object_name,
            schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
            target_dialect=target_dialect,
        )

    async def get_object_ddl(
        self,
        driver: Any,
        object_name: str,
        *,
        schema: "str | None" = None,
        object_type: str = "table",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "DDLResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_object_ddl(
            primary,
            object_name,
            schema=schema,
            object_type=object_type,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
            target_dialect=target_dialect,
        )

    async def get_schema_ddl(
        self,
        driver: Any,
        schema: "str | None" = None,
        *,
        include_domains: "Sequence[str] | None" = None,
        exclude_domains: "Sequence[str] | None" = None,
        exclude_tables: "Sequence[str] | None" = (),
        direction: "Literal['create', 'drop']" = "create",
        include_dependencies: bool = True,
        prefer_native: bool = True,
        redact: bool = True,
        target_dialect: "str | None" = None,
    ) -> "MetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_schema_ddl(
            primary,
            schema,
            include_domains=include_domains,
            exclude_domains=exclude_domains,
            exclude_tables=exclude_tables,
            direction=direction,
            include_dependencies=include_dependencies,
            prefer_native=prefer_native,
            redact=redact,
            target_dialect=target_dialect,
        )

    async def get_table_profile(
        self,
        driver: Any,
        table: str,
        schema: "str | None" = None,
        *,
        sample_size: int = 1000,
        target_dialect: "str | None" = None,
    ) -> "list[ColumnProfileMetadata]":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_table_profile(
            primary, table, schema, sample_size=sample_size, target_dialect=target_dialect
        )

    async def infer_table_ddl_from_data(
        self,
        driver: Any,
        table: str,
        schema: "str | None" = None,
        *,
        target_table: "str | None" = None,
        target_dialect: "str | None" = None,
        sample_size: int = 1000,
        primary_key: "str | Sequence[str] | None" = None,
    ) -> "DDLResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.infer_table_ddl_from_data(
            primary,
            table,
            schema,
            target_table=target_table,
            target_dialect=target_dialect,
            sample_size=sample_size,
            primary_key=primary_key,
        )

    async def get_system_metadata(
        self, driver: Any, request: "SystemMetadataRequest | str | None" = None, **kwargs: Any
    ) -> "SystemMetadataResult":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_system_metadata(primary, request, **kwargs)

    async def get_version(self, driver: Any) -> "VersionInfo | None":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_version(primary)

    async def get_feature_flag(self, driver: Any, feature: str) -> bool:
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_feature_flag(primary, feature)

    async def get_optimal_type(self, driver: Any, type_category: str) -> str:
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_optimal_type(primary, type_category)

    async def get_tables(self, driver: Any, schema: "str | None" = None) -> "list[TableMetadata]":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_tables(primary, schema)

    async def get_columns(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ColumnMetadata]":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_columns(primary, table, schema)

    async def get_indexes(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[IndexMetadata]":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_indexes(primary, table, schema)

    async def get_foreign_keys(
        self, driver: Any, table: "str | None" = None, schema: "str | None" = None
    ) -> "list[ForeignKeyMetadata]":
        """Delegate metadata lookup to the primary native dictionary."""
        self._routing_session._primary_only()
        primary = await self._routing_session._acquire_primary()
        return await primary.data_dictionary.get_foreign_keys(primary, table, schema)
