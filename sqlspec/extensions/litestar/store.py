"""Base session store classes for Litestar integration."""

import re
from abc import abstractmethod
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, Generic, NamedTuple, TypeVar, cast

from litestar.stores.base import Store

from sqlspec.config import LitestarConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import StoreLease, resolve_store_lease
from sqlspec.migrations.schema import (
    DropStatement,
    SchemaTarget,
    ensure_schema_async,
    ensure_schema_sync,
    metadata_has_index,
)
from sqlspec.observability import resolve_db_system
from sqlspec.utils.adapters import resolve_adapter_name
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.type_guards import has_extension_config

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

__all__ = ("BaseSQLSpecStore", "CompanionIndex")


ConfigT = TypeVar("ConfigT")
ReturnT = TypeVar("ReturnT")


logger = get_logger("sqlspec.extensions.litestar.store")


VALID_TABLE_NAME_PATTERN: Final = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
MAX_TABLE_NAME_LENGTH: Final = 63
_SQLGLOT_PARSE_DIALECTS: Final[dict[str, str]] = {"mariadb": "mysql", "mssql": "tsql", "postgresql": "postgres"}
_PROCEDURAL_DDL_PATTERN: Final = re.compile(
    r"^\s*(?:BEGIN\b|DECLARE\b|IF\s+NOT\s+EXISTS\b[\s\S]*?\bBEGIN\b)", re.IGNORECASE
)
_STANDALONE_CREATE_INDEX_PATTERN: Final = re.compile(
    r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<name>[^\s(]+)\s+ON\b", re.IGNORECASE
)
_INLINE_MYSQL_INDEX_PATTERN: Final = re.compile(
    r"\bINDEX\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\(\s*expires_at\s*\)", re.IGNORECASE
)
_PINNED_LEASE: "ContextVar[StoreLease | None]" = ContextVar("sqlspec_litestar_store_lease", default=None)


class CompanionIndex(NamedTuple):
    """Companion index definition reconciled alongside Litestar session tables."""

    name: str
    table: str
    schema: str | None
    ddl: str


class _StoreConnection(NamedTuple):
    """Connection a store operation runs on and whether the store owns its transaction."""

    connection: Any
    owned: bool


class _StoreSession(NamedTuple):
    """Database session a store operation runs on and whether the store owns its transaction."""

    session: Any
    owned: bool


class BaseSQLSpecStore(Store, Generic[ConfigT]):
    """Base class for SQLSpec-backed Litestar session stores.

    Inherits Litestar's ``Store`` abstract base class for server-side session
    storage backed by SQLSpec database adapters.

    This abstract base class provides common functionality for all database-specific
    store implementations including:
    - Connection management via SQLSpec configs
    - Session expiration calculation
    - Table creation utilities

    Inside a Litestar request that holds an open session for the store's config, store
    operations started by the request task run on that session inside the request
    transaction. In ``autocommit`` modes the request commits or rolls back the write. In
    ``manual`` mode the write joins uncommitted handler work when a transaction is open;
    otherwise the store begins its own transaction on the request session and commits it.
    After a request fails and releases its session, later store writes from the request
    task are rolled back. Outside a request, from other tasks, and after a successful
    release the store opens its own short session and commits itself.

    Subclasses must implement dialect-specific SQL queries.

    Args:
        config: SQLSpec database configuration with extension_config["litestar"] settings.
    """

    __slots__ = ("_config", "_table_name")

    adapter_name: ClassVar[str | None] = None

    extension_config_options: ClassVar[frozenset[str]] = frozenset(
        LitestarConfig.__required_keys__ | LitestarConfig.__optional_keys__
    )

    def __init__(self, config: ConfigT) -> None:
        """Initialize the session store.

        Args:
            config: SQLSpec database configuration.
        """
        self._config = config
        self._validate_extension_config()
        self._table_name = self._table_name_from_config()
        self._validate_table_name(self._table_name)

    @asynccontextmanager
    async def _store_connection(self) -> "AsyncIterator[_StoreConnection]":
        """Yield the connection a store operation runs on for the current task.

        Yields:
            The connection to use and whether the store must commit it.
        """
        lease = _current_lease(self._config)
        if lease.session is not None:
            async with _borrowed_session_async(lease) as session:
                yield _StoreConnection(session.connection, False)
            return
        if lease.discard:
            async with self._discarding_session_async() as session:
                yield _StoreConnection(session.connection, False)
            return
        async with cast("Any", self._config).provide_connection() as connection:
            yield _StoreConnection(connection, True)

    @contextmanager
    def _store_connection_sync(self) -> "Iterator[_StoreConnection]":
        """Yield the connection a store operation runs on for the current task.

        Yields:
            The connection to use and whether the store must commit it.
        """
        lease = _current_lease(self._config)
        if lease.session is not None:
            with _borrowed_session_sync(lease) as session:
                yield _StoreConnection(session.connection, False)
            return
        if lease.discard:
            with self._discarding_session_sync() as session:
                yield _StoreConnection(session.connection, False)
            return
        with cast("Any", self._config).provide_connection() as connection:
            yield _StoreConnection(connection, True)

    @asynccontextmanager
    async def _store_session(self) -> "AsyncIterator[_StoreSession]":
        """Yield the database session a store operation runs on for the current task.

        Yields:
            The database session to use and whether the store must commit it.
        """
        lease = _current_lease(self._config)
        if lease.session is not None:
            async with _borrowed_session_async(lease) as session:
                yield _StoreSession(session, False)
            return
        if lease.discard:
            async with self._discarding_session_async() as session:
                yield _StoreSession(session, False)
            return
        async with cast("Any", self._config).provide_session() as owned_session:
            yield _StoreSession(owned_session, True)

    @contextmanager
    def _store_session_sync(self) -> "Iterator[_StoreSession]":
        """Yield the database session a store operation runs on for the current task.

        Yields:
            The database session to use and whether the store must commit it.
        """
        lease = _current_lease(self._config)
        if lease.session is not None:
            with _borrowed_session_sync(lease) as session:
                yield _StoreSession(session, False)
            return
        if lease.discard:
            with self._discarding_session_sync() as session:
                yield _StoreSession(session, False)
            return
        with cast("Any", self._config).provide_session() as owned_session:
            yield _StoreSession(owned_session, True)

    @asynccontextmanager
    async def _discarding_session_async(self) -> "AsyncIterator[Any]":
        """Yield an owned session inside a transaction that is always rolled back.

        Yields:
            The owned database session.
        """
        async with cast("Any", self._config).provide_session() as session:
            await _begin_async(session)
            try:
                yield session
            finally:
                await session.rollback()

    @contextmanager
    def _discarding_session_sync(self) -> "Iterator[Any]":
        """Yield an owned session inside a transaction that is always rolled back.

        Yields:
            The owned database session.
        """
        with cast("Any", self._config).provide_session() as session:
            _begin_sync(session)
            try:
                yield session
            finally:
                session.rollback()

    async def _run_sync(self, function: "Callable[..., ReturnT]", /, *args: Any, **kwargs: Any) -> ReturnT:
        """Run a blocking store operation in a worker thread with the caller's store lease.

        The lease is resolved in the calling task, so a worker thread borrows the request
        session only when the request task itself started the operation.

        Args:
            function: Blocking store operation.
            *args: Positional arguments for ``function``.
            **kwargs: Keyword arguments for ``function``.

        Returns:
            The operation result.
        """
        lease = resolve_store_lease(self._config)

        def run() -> ReturnT:
            token = _PINNED_LEASE.set(lease)
            try:
                return function(*args, **kwargs)
            finally:
                _PINNED_LEASE.reset(token)

        return await async_(run)()

    @property
    def config(self) -> ConfigT:
        """Return the database configuration."""
        return self._config

    @property
    def table_name(self) -> str:
        """Return the session table name."""
        return self._table_name

    @property
    def create_schema_enabled(self) -> bool:
        """Return whether adapter-level creation should run."""
        manage_schema, create_schema = self._schema_management_flags()
        return manage_schema and create_schema

    @abstractmethod
    async def delete_expired(self) -> int:
        """Delete all expired sessions.

        Returns:
            Number of sessions deleted.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_table(self) -> None:
        """Create the session table if it doesn't exist."""
        raise NotImplementedError

    def prepare_schema_sync(self, driver: Any) -> None:
        """Prepare adapter-specific schema decisions with a synchronous driver."""

    async def prepare_schema_async(self, driver: Any) -> None:
        """Prepare adapter-specific schema decisions with an asynchronous driver."""

    def get_schema_targets(self) -> "list[SchemaTarget]":
        """Return ordered SchemaTarget definitions for this session store."""
        return [self._schema_target()]

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Extract standalone or inline companion index definitions from store DDL."""
        ddl = self._table_ddl()
        if _PROCEDURAL_DDL_PATTERN.match(ddl):
            return []

        raw_schema, dot, raw_table = self._table_name.partition(".")
        if dot:
            schema_name = raw_schema.strip('"`[]') or None
            bare_table = raw_table.strip('"`[]')
        else:
            schema_name = None
            bare_table = self._table_name.strip('"`[]')

        companion_indexes: list[CompanionIndex] = []
        for raw_stmt in ddl.split(";"):
            stmt = raw_stmt.strip()
            if not stmt:
                continue
            match = _STANDALONE_CREATE_INDEX_PATTERN.match(stmt)
            if match is not None:
                companion_indexes.append(
                    CompanionIndex(
                        name=match.group("name").strip('"`[]'), table=bare_table, schema=schema_name, ddl=f"{stmt};"
                    )
                )
        if companion_indexes:
            return companion_indexes

        inline_match = _INLINE_MYSQL_INDEX_PATTERN.search(ddl)
        if inline_match is not None:
            index_name = inline_match.group("name")
            index_options = getattr(self, "_index_options", "")
            return [
                CompanionIndex(
                    name=index_name,
                    table=bare_table,
                    schema=schema_name,
                    ddl=f"CREATE INDEX {index_name} ON {self._table_name} (expires_at){index_options}",
                )
            ]
        return []

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create any missing companion indexes using the active sync driver."""
        for index in self.get_companion_indexes():
            rows = driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                driver.execute_script(index.ddl)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create any missing companion indexes using the active async driver."""
        for index in self.get_companion_indexes():
            rows = await driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                await driver.execute_script(index.ddl)

    async def reconcile_schema(self, *, assume_existing: bool = False) -> None:
        """Apply additive session-table changes from the canonical store DDL.

        Args:
            assume_existing: Skip table discovery after adapter-level creation.
        """
        manage_schema, create_schema = self._schema_management_flags()
        if not manage_schema:
            return
        session_context = cast("Any", self._config).provide_session()
        if hasattr(session_context, "__aenter__"):
            async with session_context as driver:
                await self.prepare_schema_async(driver)
                targets = self.get_schema_targets()
                await ensure_schema_async(
                    driver, targets, manage_schema=True, create_schema=create_schema, assume_existing=assume_existing
                )
            return
        await async_(_reconcile_schema_sync)(self, create_schema=create_schema, assume_existing=assume_existing)

    def downgrade_drops(self) -> "list[DropStatement]":
        """Return the extension-downgrade drops for the session table and its indexes."""
        return [DropStatement(statement) for statement in self._drop_table_sql()]

    def _schema_target(self) -> SchemaTarget:
        """Build the primary session table SchemaTarget from store DDL."""
        statement_config = getattr(self._config, "statement_config", None)
        raw_dialect = getattr(self, "_dialect", None) or getattr(statement_config, "dialect", None)
        dialect = (
            _SQLGLOT_PARSE_DIALECTS.get(raw_dialect.lower(), raw_dialect)
            if isinstance(raw_dialect, str)
            else raw_dialect
        )
        return SchemaTarget.from_ddl(self._table_name, self._table_ddl(), dialect=dialect)

    def _table_name_from_config(self) -> str:
        """Extract the configured session table name."""
        default_name = "litestar_session"
        if has_extension_config(self._config):
            extension_config = cast("dict[str, dict[str, Any]]", self._config.extension_config)
            litestar_config: dict[str, Any] = extension_config.get("litestar", {})
            session_table = litestar_config.get("session_table", default_name)
            if session_table is True:
                return default_name
            return str(session_table)
        return default_name

    def _validate_extension_config(self) -> None:
        """Reject Litestar options that this adapter store cannot honor."""
        if not has_extension_config(self._config):
            return
        extension_config = cast("dict[str, dict[str, Any]]", self._config.extension_config)
        settings = extension_config.get("litestar", {})
        unsupported = sorted(set(settings).difference(type(self).extension_config_options))
        if unsupported:
            adapter = resolve_adapter_name(self) or type(self).__name__
            keys = ", ".join(repr(key) for key in unsupported)
            msg = f"Unsupported Litestar configuration key(s) for {adapter}: {keys}"
            raise ImproperConfigurationError(msg)

    def _schema_management_flags(self) -> "tuple[bool, bool]":
        """Return automatic-management and missing-table creation flags."""
        if not has_extension_config(self._config):
            return True, True
        extension_config = cast("dict[str, dict[str, Any]]", self._config.extension_config)
        settings = extension_config.get("litestar", {})
        return bool(settings.get("manage_schema", True)), bool(settings.get("create_schema", True))

    @abstractmethod
    def _table_ddl(self) -> str:
        """Get the CREATE TABLE SQL for this database dialect.

        Returns:
            SQL statement to create the sessions table.
        """
        raise NotImplementedError

    @abstractmethod
    def _drop_table_sql(self) -> "list[str]":
        """Get the DROP TABLE SQL statements for this database dialect.

        Returns:
            List of SQL statements to drop the table and all indexes.
            Order matters: drop indexes before table.
        """
        raise NotImplementedError

    def _log_table_created(self) -> None:
        logger.debug(
            "Litestar session table ready",
            extra={"db.system": resolve_db_system(type(self).__name__), "session_table": self._table_name},
        )

    def _log_delete_all(self) -> None:
        logger.debug(
            "Litestar sessions cleared",
            extra={"db.system": resolve_db_system(type(self).__name__), "session_table": self._table_name},
        )

    def _log_delete_expired(self, count: int) -> None:
        logger.debug(
            "Litestar sessions expired cleanup",
            extra={
                "db.system": resolve_db_system(type(self).__name__),
                "session_table": self._table_name,
                "deleted_sessions": count,
            },
        )

    def _calculate_expires_at(self, expires_in: "int | timedelta | None") -> "datetime | None":
        """Calculate expiration timestamp from ``expires_in``.

        Non-positive durations return an already-expired UTC timestamp rather than
        ``None`` so sessions expire immediately instead of becoming persistent.

        Args:
            expires_in: Seconds or timedelta until expiration, or ``None`` for no expiration.

        Returns:
            UTC datetime of expiration, or ``None`` when ``expires_in`` is ``None``.
        """
        if expires_in is None:
            return None

        expires_in_seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)

        if expires_in_seconds <= 0:
            return datetime.now(UTC) - timedelta(seconds=1)

        return datetime.now(UTC) + timedelta(seconds=expires_in_seconds)

    def _is_renewal_requested(self, renew_for: "int | timedelta | None") -> bool:
        """Return whether ``renew_for`` requests session expiration renewal.

        Args:
            renew_for: Seconds or timedelta to extend expiration by, or ``None``.

        Returns:
            ``True`` only when ``renew_for`` represents a positive duration.
        """
        if renew_for is None:
            return False
        return (renew_for.total_seconds() if isinstance(renew_for, timedelta) else float(renew_for)) > 0

    def _value_to_bytes(self, value: "str | bytes") -> bytes:
        """Convert value to bytes if needed.

        Args:
            value: String or bytes value.

        Returns:
            Value as bytes.
        """
        if isinstance(value, str):
            return value.encode("utf-8")
        return value

    def _validate_table_name(self, table_name: str) -> None:
        """Validate the configured session table name; adapters may accept qualified names.

        Args:
            table_name: Table name to validate.
        """
        self._ensure_table_name(table_name)

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        """Validate table name for SQL safety.

        Args:
            table_name: Table name to validate.

        Raises:
            ValueError: If table name is invalid.
        """
        if not table_name:
            msg = "Table name cannot be empty"
            raise ValueError(msg)

        if len(table_name) > MAX_TABLE_NAME_LENGTH:
            msg = f"Table name too long: {len(table_name)} chars (max {MAX_TABLE_NAME_LENGTH})"
            raise ValueError(msg)

        if not VALID_TABLE_NAME_PATTERN.fullmatch(table_name):
            msg = f"Invalid table name: {table_name!r}. Must start with letter/underscore and contain only alphanumeric characters and underscores"
            raise ValueError(msg)


def _reconcile_schema_sync(store: "BaseSQLSpecStore[Any]", *, create_schema: bool, assume_existing: bool) -> None:
    """Run additive reconciliation for a synchronous Litestar store."""
    with cast("Any", store.config).provide_session() as driver:
        store.prepare_schema_sync(driver)
        targets = store.get_schema_targets()
        ensure_schema_sync(
            driver, targets, manage_schema=True, create_schema=create_schema, assume_existing=assume_existing
        )


async def _begin_async(session: Any) -> None:
    if not session._connection_in_transaction():
        await session.begin()


def _begin_sync(session: Any) -> None:
    if not session._connection_in_transaction():
        session.begin()


def _current_lease(config: Any) -> StoreLease:
    pinned = _PINNED_LEASE.get()
    return pinned if pinned is not None else resolve_store_lease(config)


@asynccontextmanager
async def _borrowed_session_async(lease: StoreLease) -> "AsyncIterator[Any]":
    """Yield the borrowed request session inside a transaction.

    The store joins an open request transaction. When none is open it begins one; in
    ``manual`` commit mode the store owns that transaction and commits it, otherwise the
    request commits or rolls it back.

    Yields:
        The borrowed database session.
    """
    session = lease.session
    began = not session._connection_in_transaction()
    if began:
        await session.begin()
    if not (began and lease.manual):
        yield session
        return
    try:
        yield session
    except BaseException:
        await session.rollback()
        raise
    await session.commit()


@contextmanager
def _borrowed_session_sync(lease: StoreLease) -> "Iterator[Any]":
    """Yield the borrowed request session inside a transaction.

    The store joins an open request transaction. When none is open it begins one; in
    ``manual`` commit mode the store owns that transaction and commits it, otherwise the
    request commits or rolls it back.

    Yields:
        The borrowed database session.
    """
    session = lease.session
    began = not session._connection_in_transaction()
    if began:
        session.begin()
    if not (began and lease.manual):
        yield session
        return
    try:
        yield session
    except BaseException:
        session.rollback()
        raise
    session.commit()
