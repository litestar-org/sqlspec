"""Register SQLSpecPlugin with a Litestar application and inject a named database session."""

from typing import Any

from litestar import Litestar, get
from litestar.di import NamedDependency

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.litestar import SQLSpecPlugin

__all__ = ("health_check", "main")


@get("/health", sync_to_thread=False)
def health_check(db_session: NamedDependency[SqliteDriver]) -> dict[str, Any]:
    """Execute a health-check query against the injected SQLite session."""
    return db_session.select_one("SELECT 'ok' AS status")


def main() -> Litestar:
    """Create a Litestar application wired with SQLSpecPlugin."""
    spec = SQLSpec()
    spec.add_config(
        SqliteConfig(
            connection_config={"database": ":memory:"}, extension_config={"litestar": {"session_key": "db_session"}}
        )
    )
    return Litestar(route_handlers=[health_check], plugins=[SQLSpecPlugin(sqlspec=spec)])


if __name__ == "__main__":
    print(main())
