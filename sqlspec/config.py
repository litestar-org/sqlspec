from abc import ABC, abstractmethod
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Generic, Literal, TypeVar, cast

from typing_extensions import NotRequired, TypedDict

from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.migrations.tracker import AsyncMigrationTracker, SyncMigrationTracker
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Awaitable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.loader import SQLFileLoader
    from sqlspec.migrations.commands import AsyncMigrationCommands, SyncMigrationCommands


__all__ = (
    "AsyncConfigT",
    "AsyncDatabaseConfig",
    "ConfigT",
    "DatabaseConfigProtocol",
    "DriverT",
    "LifecycleConfig",
    "LitestarConfig",
    "MigrationConfig",
    "NoPoolAsyncConfig",
    "NoPoolSyncConfig",
    "SyncConfigT",
    "SyncDatabaseConfig",
)

AsyncConfigT = TypeVar("AsyncConfigT", bound="AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]")
SyncConfigT = TypeVar("SyncConfigT", bound="SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]")
ConfigT = TypeVar(
    "ConfigT",
    bound="AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]",
)

# Define TypeVars for Generic classes
ConnectionT = TypeVar("ConnectionT")
PoolT = TypeVar("PoolT")
DriverT = TypeVar("DriverT", bound="SyncDriverAdapterBase | AsyncDriverAdapterBase")

logger = get_logger("config")


class LifecycleConfig(TypedDict):
    """Lifecycle hooks for database adapters.

    Each hook accepts a list of callables to support multiple handlers.
    """

    on_connection_create: NotRequired[list[Callable[[Any], None]]]
    on_connection_destroy: NotRequired[list[Callable[[Any], None]]]
    on_pool_create: NotRequired[list[Callable[[Any], None]]]
    on_pool_destroy: NotRequired[list[Callable[[Any], None]]]
    on_session_start: NotRequired[list[Callable[[Any], None]]]
    on_session_end: NotRequired[list[Callable[[Any], None]]]
    on_query_start: NotRequired[list[Callable[[str, dict], None]]]
    on_query_complete: NotRequired[list[Callable[[str, dict, Any], None]]]
    on_error: NotRequired[list[Callable[[Exception, str, dict], None]]]


class MigrationConfig(TypedDict):
    """Configuration options for database migrations.

    All fields are optional with default values.
    """

    script_location: NotRequired[str]
    """Path to the migrations directory. Defaults to 'migrations'."""

    version_table_name: NotRequired[str]
    """Name of the table used to track applied migrations. Defaults to 'sqlspec_migrations'."""

    project_root: NotRequired[str]
    """Path to the project root directory. Used for relative path resolution."""

    enabled: NotRequired[bool]
    """Whether this configuration should be included in CLI operations. Defaults to True."""


class LitestarConfig(TypedDict):
    """Configuration options for Litestar SQLSpec plugin.

    All fields are optional with sensible defaults.
    """

    connection_key: NotRequired[str]
    """Key for storing connection in ASGI scope. Default: 'db_connection'"""

    pool_key: NotRequired[str]
    """Key for storing connection pool in application state. Default: 'db_pool'"""

    session_key: NotRequired[str]
    """Key for storing session in ASGI scope. Default: 'db_session'"""

    commit_mode: NotRequired[Literal["manual", "autocommit", "autocommit_include_redirect"]]
    """Transaction commit mode. Default: 'manual'"""

    enable_correlation_middleware: NotRequired[bool]
    """Enable request correlation ID middleware. Default: True"""

    extra_commit_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger commit. Default: set()"""

    extra_rollback_statuses: NotRequired[set[int]]
    """Additional HTTP status codes that trigger rollback. Default: set()"""


class DatabaseConfigProtocol(ABC, Generic[ConnectionT, PoolT, DriverT]):
    """Protocol defining the interface for database configurations."""

    __slots__ = (
        "_migration_commands",
        "_migration_loader",
        "bind_key",
        "driver_features",
        "migration_config",
        "pool_instance",
        "statement_config",
    )

    _migration_loader: "SQLFileLoader"
    _migration_commands: "SyncMigrationCommands | AsyncMigrationCommands"
    driver_type: "ClassVar[type[Any]]"
    connection_type: "ClassVar[type[Any]]"
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    supports_native_arrow_import: "ClassVar[bool]" = False
    supports_native_arrow_export: "ClassVar[bool]" = False
    supports_native_parquet_import: "ClassVar[bool]" = False
    supports_native_parquet_export: "ClassVar[bool]" = False
    bind_key: "str | None"
    statement_config: "StatementConfig"
    pool_instance: "PoolT | None"
    migration_config: "dict[str, Any] | MigrationConfig"
    driver_features: "dict[str, Any]"

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return bool(self.pool_instance == other.pool_instance and self.migration_config == other.migration_config)

    def __repr__(self) -> str:
        parts = ", ".join([f"pool_instance={self.pool_instance!r}", f"migration_config={self.migration_config!r}"])
        return f"{type(self).__name__}({parts})"

    @abstractmethod
    def create_connection(self) -> "ConnectionT | Awaitable[ConnectionT]":
        """Create and return a new database connection."""
        raise NotImplementedError

    @abstractmethod
    def provide_connection(
        self, *args: Any, **kwargs: Any
    ) -> "AbstractContextManager[ConnectionT] | AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    @abstractmethod
    def provide_session(
        self, *args: Any, **kwargs: Any
    ) -> "AbstractContextManager[DriverT] | AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    @abstractmethod
    def create_pool(self) -> "PoolT | Awaitable[PoolT]":
        """Create and return connection pool."""
        raise NotImplementedError

    @abstractmethod
    def close_pool(self) -> "Awaitable[None] | None":
        """Terminate the connection pool."""
        raise NotImplementedError

    @abstractmethod
    def provide_pool(
        self, *args: Any, **kwargs: Any
    ) -> "PoolT | Awaitable[PoolT] | AbstractContextManager[PoolT] | AbstractAsyncContextManager[PoolT]":
        """Provide pool instance."""
        raise NotImplementedError

    def get_signature_namespace(self) -> "dict[str, type[Any]]":
        """Get the signature namespace for this database configuration.

        Returns a dictionary of type names to types that should be registered
        with Litestar's signature namespace to prevent serialization attempts
        on database-specific types.

        Returns:
            Dictionary mapping type names to types.
        """
        return {}

    def _initialize_migration_components(self) -> None:
        """Initialize migration loader and commands with necessary imports.

        Handles the circular import between config and commands by importing
        at runtime when needed.
        """
        from sqlspec.loader import SQLFileLoader
        from sqlspec.migrations.commands import create_migration_commands

        self._migration_loader = SQLFileLoader()
        self._migration_commands = create_migration_commands(self)  # pyright: ignore

    def _ensure_migration_loader(self) -> "SQLFileLoader":
        """Get the migration SQL loader and auto-load files if needed.

        Returns:
            SQLFileLoader instance for migration files.
        """
        # Auto-load migration files from configured migration path if it exists
        migration_config = self.migration_config or {}
        script_location = migration_config.get("script_location", "migrations")

        from pathlib import Path

        migration_path = Path(script_location)
        if migration_path.exists() and not self._migration_loader.list_files():
            self._migration_loader.load_sql(migration_path)
            logger.debug("Auto-loaded migration SQL files from %s", migration_path)

        return self._migration_loader

    def _ensure_migration_commands(self) -> "SyncMigrationCommands | AsyncMigrationCommands":
        """Get the migration commands instance.

        Returns:
            MigrationCommands instance for this config.
        """
        return self._migration_commands

    def get_migration_loader(self) -> "SQLFileLoader":
        """Get the SQL loader for migration files.

        Provides access to migration SQL files loaded from the configured
        script_location directory. Files are loaded lazily on first access.

        Returns:
            SQLFileLoader instance with migration files loaded.
        """
        return self._ensure_migration_loader()

    def load_migration_sql_files(self, *paths: "str | Path") -> None:
        """Load additional migration SQL files from specified paths.

        Args:
            *paths: One or more file paths or directory paths to load migration SQL files from.
        """

        loader = self._ensure_migration_loader()
        for path in paths:
            path_obj = Path(path)
            if path_obj.exists():
                loader.load_sql(path_obj)
                logger.debug("Loaded migration SQL files from %s", path_obj)
            else:
                logger.warning("Migration path does not exist: %s", path_obj)

    def get_migration_commands(self) -> "SyncMigrationCommands | AsyncMigrationCommands":
        """Get migration commands for this configuration.

        Returns:
            MigrationCommands instance configured for this database.
        """
        return self._ensure_migration_commands()

    async def migrate_up(self, revision: str = "head") -> None:
        """Apply migrations up to the specified revision.

        Args:
            revision: Target revision or "head" for latest. Defaults to "head".
        """
        commands = self._ensure_migration_commands()

        await cast("AsyncMigrationCommands", commands).upgrade(revision)

    async def migrate_down(self, revision: str = "-1") -> None:
        """Apply migrations down to the specified revision.

        Args:
            revision: Target revision, "-1" for one step back, or "base" for all migrations. Defaults to "-1".
        """
        commands = self._ensure_migration_commands()

        await cast("AsyncMigrationCommands", commands).downgrade(revision)

    async def get_current_migration(self, verbose: bool = False) -> "str | None":
        """Get the current migration version.

        Args:
            verbose: Whether to show detailed migration history.

        Returns:
            Current migration version or None if no migrations applied.
        """
        commands = self._ensure_migration_commands()

        return await cast("AsyncMigrationCommands", commands).current(verbose=verbose)

    async def create_migration(self, message: str, file_type: str = "sql") -> None:
        """Create a new migration file.

        Args:
            message: Description for the migration.
            file_type: Type of migration file to create ('sql' or 'py'). Defaults to 'sql'.
        """
        commands = self._ensure_migration_commands()

        await cast("AsyncMigrationCommands", commands).revision(message, file_type)

    async def init_migrations(self, directory: "str | None" = None, package: bool = True) -> None:
        """Initialize migration directory structure.

        Args:
            directory: Directory to initialize migrations in. Uses script_location from migration_config if not provided.
            package: Whether to create __init__.py file. Defaults to True.
        """
        if directory is None:
            migration_config = self.migration_config or {}
            directory = migration_config.get("script_location") or "migrations"

        commands = self._ensure_migration_commands()
        assert directory is not None

        await cast("AsyncMigrationCommands", commands).init(directory, package)


class NoPoolSyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for sync database configurations that do not implement a pool."""

    __slots__ = ("connection_config", "extension_config")
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = False
    migration_tracker_type: "ClassVar[type[Any]]" = SyncMigrationTracker

    def __init__(
        self,
        *,
        connection_config: dict[str, Any] | None = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any]] | None" = None,
    ) -> None:
        self.bind_key = bind_key
        self.pool_instance = None
        self.connection_config = connection_config or {}
        self.extension_config: dict[str, dict[str, Any]] = extension_config or {}
        self.migration_config: dict[str, Any] | MigrationConfig = migration_config or {}
        self._initialize_migration_components()

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="sqlite", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}

    def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    def create_pool(self) -> None:
        return None

    def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class NoPoolAsyncConfig(DatabaseConfigProtocol[ConnectionT, None, DriverT]):
    """Base class for async database configurations that do not implement a pool."""

    __slots__ = ("connection_config", "extension_config")
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = False
    migration_tracker_type: "ClassVar[type[Any]]" = AsyncMigrationTracker

    def __init__(
        self,
        *,
        connection_config: "dict[str, Any] | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any]] | None" = None,
    ) -> None:
        self.bind_key = bind_key
        self.pool_instance = None
        self.connection_config = connection_config or {}
        self.extension_config: dict[str, dict[str, Any]] = extension_config or {}
        self.migration_config: dict[str, Any] | MigrationConfig = migration_config or {}
        self._initialize_migration_components()

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="sqlite", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}

    async def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    async def create_pool(self) -> None:
        return None

    async def close_pool(self) -> None:
        return None

    def provide_pool(self, *args: Any, **kwargs: Any) -> None:
        return None


class SyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Base class for sync database configurations with connection pooling."""

    __slots__ = ("extension_config", "pool_config")
    is_async: "ClassVar[bool]" = False
    supports_connection_pooling: "ClassVar[bool]" = True
    migration_tracker_type: "ClassVar[type[Any]]" = SyncMigrationTracker

    def __init__(
        self,
        *,
        pool_config: "dict[str, Any] | None" = None,
        pool_instance: "PoolT | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any]] | None" = None,
    ) -> None:
        self.bind_key = bind_key
        self.pool_instance = pool_instance
        self.pool_config = pool_config or {}
        self.extension_config: dict[str, dict[str, Any]] = extension_config or {}
        self.migration_config: dict[str, Any] | MigrationConfig = migration_config or {}
        self._initialize_migration_components()

        if statement_config is None:
            default_parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
            )
            self.statement_config = StatementConfig(dialect="postgres", parameter_config=default_parameter_config)
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}

    def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        if self.pool_instance is not None:
            return self.pool_instance
        self.pool_instance = self._create_pool()
        return self.pool_instance

    def close_pool(self) -> None:
        """Close the connection pool."""
        self._close_pool()

    def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.pool_instance is None:
            self.pool_instance = self.create_pool()
        return self.pool_instance

    def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    @abstractmethod
    def _create_pool(self) -> PoolT:
        """Actual pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    def _close_pool(self) -> None:
        """Actual pool destruction implementation."""
        raise NotImplementedError


class AsyncDatabaseConfig(DatabaseConfigProtocol[ConnectionT, PoolT, DriverT]):
    """Base class for async database configurations with connection pooling."""

    __slots__ = ("extension_config", "pool_config")
    is_async: "ClassVar[bool]" = True
    supports_connection_pooling: "ClassVar[bool]" = True
    migration_tracker_type: "ClassVar[type[Any]]" = AsyncMigrationTracker

    def __init__(
        self,
        *,
        pool_config: "dict[str, Any] | None" = None,
        pool_instance: "PoolT | None" = None,
        migration_config: "dict[str, Any] | MigrationConfig | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "dict[str, dict[str, Any]] | None" = None,
    ) -> None:
        self.bind_key = bind_key
        self.pool_instance = pool_instance
        self.pool_config = pool_config or {}
        self.extension_config: dict[str, dict[str, Any]] = extension_config or {}
        self.migration_config: dict[str, Any] | MigrationConfig = migration_config or {}
        self._initialize_migration_components()

        if statement_config is None:
            self.statement_config = StatementConfig(
                parameter_config=ParameterStyleConfig(
                    default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
                ),
                dialect="postgres",
            )
        else:
            self.statement_config = statement_config
        self.driver_features = driver_features or {}

    async def create_pool(self) -> PoolT:
        """Create and return the connection pool.

        Returns:
            The created pool.
        """
        if self.pool_instance is not None:
            return self.pool_instance
        self.pool_instance = await self._create_pool()
        return self.pool_instance

    async def close_pool(self) -> None:
        """Close the connection pool."""
        await self._close_pool()

    async def provide_pool(self, *args: Any, **kwargs: Any) -> PoolT:
        """Provide pool instance."""
        if self.pool_instance is None:
            self.pool_instance = await self.create_pool()
        return self.pool_instance

    async def create_connection(self) -> ConnectionT:
        """Create a database connection."""
        raise NotImplementedError

    def provide_connection(self, *args: Any, **kwargs: Any) -> "AbstractAsyncContextManager[ConnectionT]":
        """Provide a database connection context manager."""
        raise NotImplementedError

    def provide_session(
        self, *args: Any, statement_config: "StatementConfig | None" = None, **kwargs: Any
    ) -> "AbstractAsyncContextManager[DriverT]":
        """Provide a database session context manager."""
        raise NotImplementedError

    @abstractmethod
    async def _create_pool(self) -> PoolT:
        """Actual async pool creation implementation."""
        raise NotImplementedError

    @abstractmethod
    async def _close_pool(self) -> None:
        """Actual async pool destruction implementation."""
        raise NotImplementedError
