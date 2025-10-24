from typing import TYPE_CHECKING, Any

from fastapi import Request

from sqlspec.extensions.starlette.extension import SQLSpecPlugin as _StarlettePlugin

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core.filters import FilterTypes
    from sqlspec.extensions.fastapi.providers import DependencyDefaults, FilterConfig

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
            pool_config={"dsn": "postgresql://localhost/mydb"},
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
        async def list_users(db = Depends(db_ext.session_dependency())):
            result = await db.execute("SELECT * FROM users")
            return {"users": result.all()}
    """

    def session_dependency(self, key: "str | None" = None) -> "Callable[[Request], Any]":
        """Create dependency factory for session injection.

        Returns a callable that can be used with FastAPI's Depends() to inject
        a database session into route handlers.

        Args:
            key: Optional session key for multi-database configurations.

        Returns:
            Dependency callable for FastAPI Depends().

        Example:
            @app.get("/users")
            async def get_users(db = Depends(db_ext.session_dependency())):
                return await db.execute("SELECT * FROM users")

            @app.get("/products")
            async def get_products(db = Depends(db_ext.session_dependency("products"))):
                return await db.execute("SELECT * FROM products")
        """

        def dependency(request: Request) -> Any:
            return self.get_session(request, key)

        return dependency

    def connection_dependency(self, key: "str | None" = None) -> "Callable[[Request], Any]":
        """Create dependency factory for connection injection.

        Returns a callable that can be used with FastAPI's Depends() to inject
        a database connection into route handlers.

        Args:
            key: Optional session key for multi-database configurations.

        Returns:
            Dependency callable for FastAPI Depends().

        Example:
            @app.get("/raw")
            async def raw_query(conn = Depends(db_ext.connection_dependency())):
                cursor = await conn.cursor()
                await cursor.execute("SELECT 1")
                return await cursor.fetchone()
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
                db = Depends(db_ext.session_dependency()),
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
        from sqlspec.extensions.fastapi.providers import DEPENDENCY_DEFAULTS
        from sqlspec.extensions.fastapi.providers import provide_filters as _provide_filters

        if dep_defaults is None:
            dep_defaults = DEPENDENCY_DEFAULTS

        return _provide_filters(config, dep_defaults=dep_defaults)
