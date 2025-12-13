from typing import TYPE_CHECKING, Any, overload

from fastapi import FastAPI, Request

from sqlspec.base import SQLSpec
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

    Example:
        from fastapi import Depends, FastAPI
        from sqlspec import SQLSpec
        from sqlspec.adapters.asyncpg import AsyncpgConfig
        from sqlspec.extensions.fastapi import SQLSpecPlugin

        sqlspec = SQLSpec()
        config = AsyncpgConfig(
            connection_config={"dsn": "postgresql://localhost/mydb"},
            extension_config={
                "starlette": {
                    "commit_mode": "autocommit",
                    "session_key": "db"
                }
            }
        )
        sqlspec.add_config(config, name="default")

        app = FastAPI()
        db_ext = SQLSpecPlugin(sqlspec, app)

        @app.get("/users")
        async def list_users(db = Depends(db_ext.provide_session())):
            result = await db.execute("SELECT * FROM users")
            return {"users": result.all()}
    """

    def __init__(self, sqlspec: SQLSpec, app: "FastAPI | None" = None) -> None:
        """Initialize SQLSpec FastAPI extension.

        Args:
            sqlspec: Pre-configured SQLSpec instance with registered configs.
            app: Optional FastAPI application to initialize immediately.
        """
        super().__init__(sqlspec, app)

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

        Example:
            # No args - returns union type
            @app.get("/users")
            async def get_users(db = Depends(db_ext.provide_session())):
                return await db.execute("SELECT * FROM users")

            # String key for multi-database
            @app.get("/products")
            async def get_products(db = Depends(db_ext.provide_session("products"))):
                return await db.execute("SELECT * FROM products")

            # Config instance for type narrowing
            config = AsyncpgConfig(...)
            @app.get("/typed")
            async def typed_query(db = Depends(db_ext.provide_session(config))):
                # db is properly typed as AsyncDriverAdapterBase
                return await db.execute("SELECT 1")

            # Config type/class for type narrowing
            @app.get("/typed2")
            async def typed_query2(db = Depends(db_ext.provide_session(AsyncpgConfig))):
                # db is properly typed as AsyncDriverAdapterBase
                return await db.execute("SELECT 1")
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

        Example:
            @app.get("/users")
            async def get_users(db = Depends(db_ext.provide_async_session())):
                # db is AsyncDriverAdapterBase
                return await db.execute("SELECT * FROM users")

            @app.get("/products")
            async def get_products(db = Depends(db_ext.provide_async_session("products_db"))):
                # db is AsyncDriverAdapterBase for "products_db" key
                return await db.execute("SELECT * FROM products")
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

        Example:
            @app.get("/users")
            def get_users(db = Depends(db_ext.provide_sync_session())):
                # db is SyncDriverAdapterBase
                return db.execute("SELECT * FROM users")
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

        Example:
            # No args
            @app.get("/raw")
            async def raw_query(conn = Depends(db_ext.provide_connection())):
                cursor = await conn.cursor()
                await cursor.execute("SELECT 1")
                return await cursor.fetchone()

            # With config instance
            config = AsyncpgConfig(...)
            @app.get("/typed")
            async def typed_query(conn = Depends(db_ext.provide_connection(config))):
                cursor = await conn.cursor()
                await cursor.execute("SELECT 1")
                return await cursor.fetchone()

            # With config type/class
            @app.get("/typed2")
            async def typed_query2(conn = Depends(db_ext.provide_connection(AsyncpgConfig))):
                cursor = await conn.cursor()
                await cursor.execute("SELECT 1")
                return await cursor.fetchone()
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

        Example:
            @app.get("/raw")
            async def raw_query(conn = Depends(db_ext.provide_async_connection())):
                cursor = await conn.cursor()
                await cursor.execute("SELECT 1")
                return await cursor.fetchone()
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

        Example:
            @app.get("/raw")
            def raw_query(conn = Depends(db_ext.provide_sync_connection())):
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return cursor.fetchone()
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

        Example:
            from fastapi import Depends
            from sqlspec.extensions.fastapi import FilterConfig

            @app.get("/users")
            async def list_users(
                db = Depends(db_ext.provide_session()),
                filters = Depends(
                    db_ext.provide_filters({
                        "id_filter": UUID,
                        "search": "name,email",
                        "search_ignore_case": True,
                        "pagination_type": "limit_offset",
                        "sort_field": "created_at",
                    })
                ),
            ):
                stmt = sql("SELECT * FROM users")
                for filter in filters:
                    stmt = filter.append_to_statement(stmt)
                result = await db.execute(stmt)
                return result.all()
        """

        if dep_defaults is None:
            dep_defaults = DEPENDENCY_DEFAULTS

        return _provide_filters(config, dep_defaults=dep_defaults)
