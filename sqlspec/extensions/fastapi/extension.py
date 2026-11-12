"""FastAPI extension for SQLSpec database session and dependency management."""

from typing import TYPE_CHECKING, Any, overload

from fastapi import Request

from sqlspec.extensions.fastapi.providers import DEPENDENCY_DEFAULTS
from sqlspec.extensions.fastapi.providers import provide_filters as _provide_filters
from sqlspec.extensions.starlette.extension import SQLSpecPlugin as _StarlettePlugin

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from sqlspec.config import AsyncDatabaseConfig, DriverT, NoPoolAsyncConfig, NoPoolSyncConfig, SyncDatabaseConfig
    from sqlspec.core import FilterTypes
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.extensions.fastapi.providers import DependencyDefaults, FilterConfig

    _AsyncSession = AsyncDriverAdapterBase
    _SyncSession = SyncDriverAdapterBase
    _Session = AsyncDriverAdapterBase | SyncDriverAdapterBase
else:
    _AsyncSession = Any
    _SyncSession = Any
    _Session = Any

__all__ = ("SQLSpecPlugin",)


class SQLSpecPlugin(_StarlettePlugin):
    """SQLSpec integration for FastAPI applications.

    Extends Starlette integration with dependency injection helpers for FastAPI's
    Depends() system.
    """

    @overload
    def provide_session(
        self, key: None = None
    ) -> "Callable[[Request], Awaitable[AsyncDriverAdapterBase | SyncDriverAdapterBase]]": ...
    @overload
    def provide_session(
        self, key: str
    ) -> "Callable[[Request], Awaitable[AsyncDriverAdapterBase | SyncDriverAdapterBase]]": ...
    @overload
    def provide_session(
        self, key: "type[AsyncDatabaseConfig[Any, Any, DriverT] | NoPoolAsyncConfig[Any, DriverT]]"
    ) -> "Callable[[Request], Awaitable[DriverT]]": ...
    @overload
    def provide_session(
        self, key: "type[SyncDatabaseConfig[Any, Any, DriverT] | NoPoolSyncConfig[Any, DriverT]]"
    ) -> "Callable[[Request], Awaitable[DriverT]]": ...
    @overload
    def provide_session(
        self, key: "AsyncDatabaseConfig[Any, Any, DriverT] | NoPoolAsyncConfig[Any, DriverT]"
    ) -> "Callable[[Request], Awaitable[DriverT]]": ...
    @overload
    def provide_session(
        self, key: "SyncDatabaseConfig[Any, Any, DriverT] | NoPoolSyncConfig[Any, DriverT]"
    ) -> "Callable[[Request], Awaitable[DriverT]]": ...
    def provide_session(
        self,
        key: "str | type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | None" = None,
    ) -> "Callable[[Request], Awaitable[Any]]":
        """Create dependency factory for session injection.

        Returns an async callable compatible with FastAPI's ``Depends()`` that
        acquires deferred tenant leases on demand once an earlier dependency or
        middleware has called :func:`~sqlspec.extensions.fastapi.set_request_tenant`.

        Args:
            key: Optional session/bind key, unique config class, config instance, or None.

        Returns:
            Dependency callable for FastAPI Depends().
        """

        async def dependency(request: Request) -> _Session:
            return await self.get_session_async(request, key)  # type: ignore[no-any-return]

        return dependency

    def provide_async_session(
        self,
        key: "str | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]] | None" = None,
    ) -> "Callable[[Request], Awaitable[AsyncDriverAdapterBase]]":
        """Create dependency factory for async session injection.

        Type-narrowed version of ``provide_session()`` returning ``AsyncDriverAdapterBase``.

        Args:
            key: Optional session key, config class, or config instance.

        Returns:
            Async dependency callable returning ``AsyncDriverAdapterBase``.
        """

        async def dependency(request: Request) -> _AsyncSession:
            return await self.get_session_async(request, key)  # type: ignore[no-any-return]

        return dependency

    def provide_sync_session(
        self,
        key: "str | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]] | None" = None,
    ) -> "Callable[[Request], SyncDriverAdapterBase]":
        """Create dependency factory for sync session injection.

        Type-narrowed version of ``provide_session()`` returning ``SyncDriverAdapterBase``.

        Args:
            key: Optional session key, config class, or config instance.

        Returns:
            Dependency callable returning ``SyncDriverAdapterBase``.
        """

        def dependency(request: Request) -> _SyncSession:
            return self.get_session(request, key)  # type: ignore[no-any-return]

        return dependency

    @overload
    def provide_connection(self, key: None = None) -> "Callable[[Request], Awaitable[Any]]": ...
    @overload
    def provide_connection(self, key: str) -> "Callable[[Request], Awaitable[Any]]": ...
    @overload
    def provide_connection(
        self, key: "type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]]"
    ) -> "Callable[[Request], Awaitable[Any]]": ...
    @overload
    def provide_connection(
        self, key: "type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]]"
    ) -> "Callable[[Request], Awaitable[Any]]": ...
    @overload
    def provide_connection(
        self, key: "AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]"
    ) -> "Callable[[Request], Awaitable[Any]]": ...
    @overload
    def provide_connection(
        self, key: "SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]"
    ) -> "Callable[[Request], Awaitable[Any]]": ...
    def provide_connection(
        self,
        key: "str | type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]] | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | None" = None,
    ) -> "Callable[[Request], Awaitable[Any]]":
        """Create dependency factory for connection injection.

        Returns an async callable compatible with FastAPI's ``Depends()`` to inject
        a database connection into route handlers.

        Args:
            key: Optional session/bind key, unique config class, config instance, or None.

        Returns:
            Dependency callable for FastAPI Depends().
        """

        async def dependency(request: Request) -> Any:
            return await self.get_connection_async(request, key)

        return dependency

    def provide_async_connection(
        self,
        key: "str | AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any] | type[AsyncDatabaseConfig[Any, Any, Any] | NoPoolAsyncConfig[Any, Any]] | None" = None,
    ) -> "Callable[[Request], Awaitable[Any]]":
        """Create dependency factory for async connection injection.

        Args:
            key: Optional session key, config class, or config instance.

        Returns:
            Async dependency callable returning the raw database connection.
        """

        async def dependency(request: Request) -> Any:
            return await self.get_connection_async(request, key)

        return dependency

    def provide_sync_connection(
        self,
        key: "str | SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any] | type[SyncDatabaseConfig[Any, Any, Any] | NoPoolSyncConfig[Any, Any]] | None" = None,
    ) -> "Callable[[Request], Any]":
        """Create dependency factory for sync connection injection.

        Args:
            key: Optional session key, config class, or config instance.

        Returns:
            Dependency callable returning the raw database connection.
        """

        def dependency(request: Request) -> Any:
            return self.get_connection(request, key)

        return dependency

    @staticmethod
    def provide_filters(
        config: "FilterConfig", dep_defaults: "DependencyDefaults | None" = None
    ) -> "Callable[..., list[FilterTypes]]":
        """Create filter dependency for FastAPI routes.

        Dynamically generates a FastAPI dependency function that parses query
        parameters into SQLSpec filter objects. The returned callable can be used
        with FastAPI's Depends() for automatic filter injection.

        Args:
            config: Filter configuration specifying which filters to enable.
            dep_defaults: Optional dependency defaults for customization.

        Returns:
            Callable for use with Depends() that returns list of filters.
        """

        if dep_defaults is None:
            dep_defaults = DEPENDENCY_DEFAULTS

        return _provide_filters(config, dep_defaults=dep_defaults)

    def _extract_extension_settings(self, config: Any) -> "dict[str, Any]":
        """Extract FastAPI settings from config.extension_config.

        Args:
            config: Database configuration instance.

        Returns:
            Dictionary of FastAPI-specific settings.
        """
        framework_config = config.extension_config.get("fastapi", {})
        pool_key = framework_config.get("pool_key", "db_pool")
        if not config.supports_connection_pooling and pool_key == "db_pool":
            pool_key = f"_db_pool_{id(config)}"
        correlation_headers = framework_config.get("correlation_headers")
        return {
            "connection_key": framework_config.get("connection_key", "db_connection"),
            "pool_key": pool_key,
            "session_key": framework_config.get("session_key", "db_session"),
            "commit_mode": framework_config.get("commit_mode", "manual"),
            "extra_commit_statuses": framework_config.get("extra_commit_statuses"),
            "extra_rollback_statuses": framework_config.get("extra_rollback_statuses"),
            "disable_di": framework_config.get("disable_di", False),
            "enable_correlation_middleware": framework_config.get("enable_correlation_middleware", True),
            "correlation_header": framework_config.get("correlation_header", "x-request-id"),
            "correlation_headers": tuple(correlation_headers) if correlation_headers is not None else None,
            "auto_trace_headers": framework_config.get("auto_trace_headers", True),
            "enable_sqlcommenter_middleware": framework_config.get("enable_sqlcommenter_middleware", True),
            "tenant_extractor": framework_config.get("tenant_extractor"),
            "sqlcommenter_framework": framework_config.get("sqlcommenter_framework", "fastapi"),
        }
