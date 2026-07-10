from typing import TYPE_CHECKING, Any, overload

from fastapi import Request

from sqlspec.extensions._framework_common import extract_extension_settings
from sqlspec.extensions.fastapi.providers import DEPENDENCY_DEFAULTS
from sqlspec.extensions.fastapi.providers import provide_filters as _provide_filters
from sqlspec.extensions.starlette.extension import SQLSpecPlugin as _StarlettePlugin

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.core import FilterTypes
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.extensions.fastapi.providers import DependencyDefaults, FilterConfig

    # Type aliases for static analysis - IDEs see the real types
    _AsyncSession = AsyncDriverAdapterBase
    _SyncSession = SyncDriverAdapterBase
    _Session = AsyncDriverAdapterBase | SyncDriverAdapterBase
else:
    # Runtime fallback - FastAPI sees Any (avoids NameError)
    _AsyncSession = Any
    _SyncSession = Any
    _Session = Any

__all__ = ("SQLSpecPlugin",)


class SQLSpecPlugin(_StarlettePlugin):
    """SQLSpec integration for FastAPI applications.

    Extends Starlette integration with dependency injection helpers for FastAPI's
    Depends() system.
    """

    def _extract_extension_settings(self, config: Any) -> "dict[str, Any]":
        """Extract FastAPI settings from config.extension_config.

        Args:
            config: Database configuration instance.

        Returns:
            Dictionary of FastAPI-specific settings.
        """
        return extract_extension_settings(config, framework_key="fastapi", sqlcommenter_framework="fastapi")

    @overload
    def provide_session(
        self, key: None = None
    ) -> "Callable[[Request], AsyncDriverAdapterBase | SyncDriverAdapterBase]": ...

    @overload
    def provide_session(self, key: str) -> "Callable[[Request], AsyncDriverAdapterBase | SyncDriverAdapterBase]": ...

    @overload
    def provide_session(self, key: "type[AsyncDatabaseConfig]") -> "Callable[[Request], AsyncDriverAdapterBase]": ...

    @overload
    def provide_session(self, key: "type[SyncDatabaseConfig]") -> "Callable[[Request], SyncDriverAdapterBase]": ...

    @overload
    def provide_session(self, key: "AsyncDatabaseConfig") -> "Callable[[Request], AsyncDriverAdapterBase]": ...

    @overload
    def provide_session(self, key: "SyncDatabaseConfig") -> "Callable[[Request], SyncDriverAdapterBase]": ...

    def provide_session(
        self,
        key: "str | type[AsyncDatabaseConfig | SyncDatabaseConfig] | AsyncDatabaseConfig | SyncDatabaseConfig | None" = None,
    ) -> "Callable[[Request], AsyncDriverAdapterBase | SyncDriverAdapterBase]":
        """Create dependency factory for session injection.

        Returns a callable that can be used with FastAPI's Depends() to inject
        a database session into route handlers.

        Args:
            key: Optional session key (str), config type for type narrowing, or None.

        Returns:
            Dependency callable for FastAPI Depends().
        """
        # Extract string key if provided, ignore config types/instances (used only for type narrowing)
        session_key = key if isinstance(key, str) or key is None else None

        def dependency(request: Request) -> _Session:
            return self.get_session(request, session_key)  # type: ignore[no-any-return]

        return dependency

    def provide_async_session(self, key: "str | None" = None) -> "Callable[[Request], AsyncDriverAdapterBase]":
        """Create dependency factory for async session injection.

        Type-narrowed version of provide_session() that returns AsyncDriverAdapterBase.
        Useful when using string keys and you know the config is async.

        Args:
            key: Optional session key for multi-database configurations.

        Returns:
            Dependency callable that returns AsyncDriverAdapterBase.
        """

        def dependency(request: Request) -> _AsyncSession:
            return self.get_session(request, key)  # type: ignore[no-any-return]

        return dependency

    def provide_sync_session(self, key: "str | None" = None) -> "Callable[[Request], SyncDriverAdapterBase]":
        """Create dependency factory for sync session injection.

        Type-narrowed version of provide_session() that returns SyncDriverAdapterBase.
        Useful when using string keys and you know the config is sync.

        Args:
            key: Optional session key for multi-database configurations.

        Returns:
            Dependency callable that returns SyncDriverAdapterBase.
        """

        def dependency(request: Request) -> _SyncSession:
            return self.get_session(request, key)  # type: ignore[no-any-return]

        return dependency

    @overload
    def provide_connection(self, key: None = None) -> "Callable[[Request], Any]": ...

    @overload
    def provide_connection(self, key: str) -> "Callable[[Request], Any]": ...

    @overload
    def provide_connection(self, key: "type[AsyncDatabaseConfig]") -> "Callable[[Request], Any]": ...

    @overload
    def provide_connection(self, key: "type[SyncDatabaseConfig]") -> "Callable[[Request], Any]": ...

    @overload
    def provide_connection(self, key: "AsyncDatabaseConfig") -> "Callable[[Request], Any]": ...

    @overload
    def provide_connection(self, key: "SyncDatabaseConfig") -> "Callable[[Request], Any]": ...

    def provide_connection(
        self,
        key: "str | type[AsyncDatabaseConfig | SyncDatabaseConfig] | AsyncDatabaseConfig | SyncDatabaseConfig | None" = None,
    ) -> "Callable[[Request], Any]":
        """Create dependency factory for connection injection.

        Returns a callable that can be used with FastAPI's Depends() to inject
        a database connection into route handlers.

        Args:
            key: Optional session key (str), config type for type narrowing, or None.

        Returns:
            Dependency callable for FastAPI Depends().
        """
        # Extract string key if provided, ignore config types/instances (used only for type narrowing)
        connection_key = key if isinstance(key, str) or key is None else None

        def dependency(request: Request) -> Any:
            return self.get_connection(request, connection_key)

        return dependency

    def provide_async_connection(self, key: "str | None" = None) -> "Callable[[Request], Any]":
        """Create dependency factory for async connection injection.

        Type-narrowed version of provide_connection() for async connections.
        Useful when using string keys and you know the config is async.

        Args:
            key: Optional session key for multi-database configurations.

        Returns:
            Dependency callable for async connection.
        """

        def dependency(request: Request) -> Any:
            return self.get_connection(request, key)

        return dependency

    def provide_sync_connection(self, key: "str | None" = None) -> "Callable[[Request], Any]":
        """Create dependency factory for sync connection injection.

        Type-narrowed version of provide_connection() for sync connections.
        Useful when using string keys and you know the config is sync.

        Args:
            key: Optional session key for multi-database configurations.

        Returns:
            Dependency callable for sync connection.
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
