"""Mock database configuration for testing with dialect transpilation.

This module provides configuration classes for the mock adapter that use
SQLite :memory: as the execution backend while accepting SQL written in
other dialects (Postgres, MySQL, Oracle, etc.).
"""

import sqlite3
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict

from typing_extensions import NotRequired

from sqlspec.adapters.mock._typing import MockAsyncSessionContext, MockConnection, MockSyncSessionContext
from sqlspec.adapters.mock.core import apply_driver_features, default_statement_config
from sqlspec.adapters.mock.driver import MockAsyncDriver, MockCursor, MockExceptionHandler, MockSyncDriver
from sqlspec.config import ExtensionConfigs, NoPoolAsyncConfig, NoPoolSyncConfig
from sqlspec.driver import convert_to_dialect
from sqlspec.utils.sync_tools import async_

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

__all__ = ("MockAsyncConfig", "MockConnectionParams", "MockDriverFeatures", "MockSyncConfig")


class MockConnectionParams(TypedDict):
    """Mock connection parameters.

    These parameters control the SQLite :memory: backend behavior.
    """

    target_dialect: NotRequired[str]
    initial_sql: NotRequired["str | list[str]"]
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: "NotRequired[str | None]"
    check_same_thread: NotRequired[bool]
    cached_statements: NotRequired[int]


class MockDriverFeatures(TypedDict):
    """Mock driver feature configuration.

    Controls optional type handling and serialization features for Mock connections.

    json_serializer: Custom JSON serializer function.
        Defaults to sqlspec.utils.serializers.to_json.
    json_deserializer: Custom JSON deserializer function.
        Defaults to sqlspec.utils.serializers.from_json.
    """

    json_serializer: "NotRequired[Callable[[Any], str]]"
    json_deserializer: "NotRequired[Callable[[str], Any]]"


class MockSyncConnectionContext:
    """Context manager for Mock sync connections."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "MockSyncConfig") -> None:
        self._config = config
        self._connection: MockConnection | None = None

    def __enter__(self) -> MockConnection:
        self._connection = self._config.create_connection()
        return self._connection

    def __exit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        return None


class MockAsyncConnectionContext:
    """Async context manager for Mock async connections."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "MockAsyncConfig") -> None:
        self._config = config
        self._connection: MockConnection | None = None

    async def __aenter__(self) -> MockConnection:
        self._connection = await self._config.create_connection()
        return self._connection

    async def __aexit__(
        self, exc_type: "type[BaseException] | None", exc_val: "BaseException | None", exc_tb: Any
    ) -> "bool | None":
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        return None


class _MockSyncSessionFactory:
    """Factory for creating mock sync sessions."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "MockSyncConfig") -> None:
        self._config = config
        self._connection: MockConnection | None = None

    def acquire_connection(self) -> MockConnection:
        self._connection = self._config.create_connection()
        return self._connection

    def release_connection(self, conn: MockConnection) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


class _MockAsyncSessionFactory:
    """Factory for creating mock async sessions."""

    __slots__ = ("_config", "_connection")

    def __init__(self, config: "MockAsyncConfig") -> None:
        self._config = config
        self._connection: MockConnection | None = None

    async def acquire_connection(self) -> MockConnection:
        self._connection = await self._config.create_connection()
        return self._connection

    async def release_connection(self, conn: MockConnection) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


class MockSyncConfig(NoPoolSyncConfig["MockConnection", "MockSyncDriver"]):
    """Sync mock database configuration.

    Uses SQLite :memory: as the execution backend with dialect transpilation.
    Write SQL in your target dialect (Postgres, MySQL, Oracle) and it will
    be transpiled to SQLite before execution.

    Example:
        config = MockSyncConfig(target_dialect="postgres")

        with config.provide_session() as session:
            session.execute(\"\"\"
                CREATE TABLE users (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100)
                )
            \"\"\")
            session.execute(
                "INSERT INTO users (name) VALUES ($1)",
                "Alice"
            )
            user = session.select_one("SELECT * FROM users WHERE name = $1", "Alice")
            assert user["name"] == "Alice"
    """

    driver_type: "ClassVar[type[MockSyncDriver]]" = MockSyncDriver
    connection_type: "ClassVar[type[MockConnection]]" = MockConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        target_dialect: str = "sqlite",
        initial_sql: "str | list[str] | None" = None,
        connection_config: "MockConnectionParams | dict[str, Any] | None" = None,
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MockDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
    ) -> None:
        """Initialize Mock sync configuration.

        Args:
            target_dialect: SQL dialect for input SQL (postgres, mysql, oracle, sqlite).
                SQL will be transpiled to SQLite before execution, unless 'sqlite'.
            initial_sql: SQL statements to execute when creating connection.
                Can be a single string or list of strings. Useful for setting up
                test fixtures.
            connection_config: Additional connection parameters.
            connection_instance: Pre-existing connection (not used for mock).
            migration_config: Migration configuration.
            statement_config: Statement configuration settings.
            driver_features: Driver feature configuration.
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration.
            observability_config: Observability configuration.
        """
        config_dict: dict[str, Any] = dict(connection_config) if connection_config else {}
        config_dict["target_dialect"] = target_dialect
        config_dict["initial_sql"] = initial_sql

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        super().__init__(
            connection_config=config_dict,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
        )

    @property
    def target_dialect(self) -> str:
        """Get the target dialect for SQL transpilation."""
        return str(self.connection_config.get("target_dialect", "sqlite"))

    @property
    def initial_sql(self) -> "str | list[str] | None":
        """Get the initial SQL to execute on connection creation."""
        return self.connection_config.get("initial_sql")

    def create_connection(self) -> MockConnection:
        """Create a new SQLite :memory: connection.

        Returns:
            SQLite connection with row factory set.
        """
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.row_factory = sqlite3.Row

        if self.initial_sql:
            self._execute_initial_sql(conn)

        return conn

    def _execute_initial_sql(self, conn: MockConnection) -> None:
        """Execute initial SQL statements on a new connection.

        Args:
            conn: SQLite connection to execute SQL on.
        """
        initial_sql = self.initial_sql
        if initial_sql is None:
            return

        statements = initial_sql if isinstance(initial_sql, list) else [initial_sql]
        target_dialect = self.target_dialect

        for sql in statements:
            if target_dialect != "sqlite":
                transpiled = convert_to_dialect(sql, target_dialect, "sqlite", pretty=False)
            else:
                transpiled = sql
            conn.executescript(transpiled)

    def provide_connection(self, *args: Any, **kwargs: Any) -> "MockSyncConnectionContext":
        """Provide a Mock sync connection context manager.

        Returns:
            Connection context manager.
        """
        return MockSyncConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "MockSyncSessionContext":
        """Provide a Mock sync driver session.

        Args:
            statement_config: Optional statement configuration override.

        Returns:
            Mock driver session context manager.
        """
        factory = _MockSyncSessionFactory(self)

        return MockSyncSessionContext(
            acquire_connection=factory.acquire_connection,
            release_connection=factory.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
            target_dialect=self.target_dialect,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for Mock types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "MockConnection": MockConnection,
            "MockConnectionParams": MockConnectionParams,
            "MockCursor": MockCursor,
            "MockDriverFeatures": MockDriverFeatures,
            "MockExceptionHandler": MockExceptionHandler,
            "MockSyncConfig": MockSyncConfig,
            "MockSyncConnectionContext": MockSyncConnectionContext,
            "MockSyncDriver": MockSyncDriver,
            "MockSyncSessionContext": MockSyncSessionContext,
        })
        return namespace


class MockAsyncConfig(NoPoolAsyncConfig["MockConnection", "MockAsyncDriver"]):
    """Async mock database configuration.

    Uses SQLite :memory: as the execution backend with dialect transpilation.
    The async interface wraps sync SQLite operations using asyncio.to_thread().

    Example:
        config = MockAsyncConfig(target_dialect="mysql")

        async with config.provide_session() as session:
            await session.execute("CREATE TABLE items (id INT, name TEXT)")
            await session.execute("INSERT INTO items VALUES (%s, %s)", 1, "Widget")
            result = await session.select("SELECT * FROM items")
            assert len(result) == 1
    """

    driver_type: "ClassVar[type[MockAsyncDriver]]" = MockAsyncDriver
    connection_type: "ClassVar[type[MockConnection]]" = MockConnection
    supports_transactional_ddl: "ClassVar[bool]" = True
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True

    def __init__(
        self,
        *,
        target_dialect: str = "sqlite",
        initial_sql: "str | list[str] | None" = None,
        connection_config: "MockConnectionParams | dict[str, Any] | None" = None,
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MockDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
    ) -> None:
        """Initialize Mock async configuration.

        Args:
            target_dialect: SQL dialect for input SQL (postgres, mysql, oracle, sqlite).
                SQL will be transpiled to SQLite before execution, unless 'sqlite'.
            initial_sql: SQL statements to execute when creating connection.
                Can be a single string or list of strings. Useful for setting up
                test fixtures.
            connection_config: Additional connection parameters.
            connection_instance: Pre-existing connection (not used for mock).
            migration_config: Migration configuration.
            statement_config: Statement configuration settings.
            driver_features: Driver feature configuration.
            bind_key: Optional unique identifier for this configuration.
            extension_config: Extension-specific configuration.
            observability_config: Observability configuration.
        """
        config_dict: dict[str, Any] = dict(connection_config) if connection_config else {}
        config_dict["target_dialect"] = target_dialect
        config_dict["initial_sql"] = initial_sql

        statement_config = statement_config or default_statement_config
        statement_config, driver_features = apply_driver_features(statement_config, driver_features)

        super().__init__(
            connection_config=config_dict,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=driver_features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
        )

    @property
    def target_dialect(self) -> str:
        """Get the target dialect for SQL transpilation."""
        return str(self.connection_config.get("target_dialect", "sqlite"))

    @property
    def initial_sql(self) -> "str | list[str] | None":
        """Get the initial SQL to execute on connection creation."""
        return self.connection_config.get("initial_sql")

    async def create_connection(self) -> MockConnection:
        """Create a new SQLite :memory: connection asynchronously.

        Returns:
            SQLite connection with row factory set.
        """
        connect_async = async_(sqlite3.connect)
        conn = await connect_async(":memory:", check_same_thread=False)
        conn.row_factory = sqlite3.Row

        if self.initial_sql:
            await self._execute_initial_sql_async(conn)

        return conn

    async def _execute_initial_sql_async(self, conn: MockConnection) -> None:
        """Execute initial SQL statements on a new connection.

        Args:
            conn: SQLite connection to execute SQL on.
        """
        initial_sql = self.initial_sql
        if initial_sql is None:
            return

        statements = initial_sql if isinstance(initial_sql, list) else [initial_sql]
        target_dialect = self.target_dialect

        for sql in statements:
            if target_dialect != "sqlite":
                transpiled = convert_to_dialect(sql, target_dialect, "sqlite", pretty=False)
            else:
                transpiled = sql
            executescript_async = async_(conn.executescript)
            await executescript_async(transpiled)

    def provide_connection(self, *args: Any, **kwargs: Any) -> "MockAsyncConnectionContext":
        """Provide a Mock async connection context manager.

        Returns:
            Async connection context manager.
        """
        return MockAsyncConnectionContext(self)

    def provide_session(
        self, *_args: Any, statement_config: "StatementConfig | None" = None, **_kwargs: Any
    ) -> "MockAsyncSessionContext":
        """Provide a Mock async driver session.

        Args:
            statement_config: Optional statement configuration override.

        Returns:
            Mock async driver session context manager.
        """
        factory = _MockAsyncSessionFactory(self)

        return MockAsyncSessionContext(
            acquire_connection=factory.acquire_connection,
            release_connection=factory.release_connection,
            statement_config=statement_config or self.statement_config or default_statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
            target_dialect=self.target_dialect,
        )

    def get_signature_namespace(self) -> "dict[str, Any]":
        """Get the signature namespace for Mock types.

        Returns:
            Dictionary mapping type names to types.
        """
        namespace = super().get_signature_namespace()
        namespace.update({
            "MockAsyncConfig": MockAsyncConfig,
            "MockAsyncConnectionContext": MockAsyncConnectionContext,
            "MockAsyncDriver": MockAsyncDriver,
            "MockAsyncSessionContext": MockAsyncSessionContext,
            "MockConnection": MockConnection,
            "MockConnectionParams": MockConnectionParams,
            "MockDriverFeatures": MockDriverFeatures,
        })
        return namespace
