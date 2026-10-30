"""Register SQLSpecPlugin with a Litestar application and inject a named database session."""

from typing import Any

from litestar import Litestar, get
from litestar.di import NamedDependency

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.extensions.litestar import SQLSpecPlugin

__all__ = ("app", "health_check", "main")


# --8<-- [start:example]
@get("/health", sync_to_thread=False)
def health_check(db_session: NamedDependency[SqliteDriver]) -> dict[str, Any]:
    """Execute a health-check query against the injected SQLite session."""
    return db_session.select_one("SELECT 'ok' AS status")


spec = SQLSpec()
spec.add_config(
    SqliteConfig(
        connection_config={"database": ":memory:"}, extension_config={"litestar": {"session_key": "db_session"}}
    )
)
app = Litestar(route_handlers=[health_check], plugins=[SQLSpecPlugin(sqlspec=spec)])
# --8<-- [end:example]


def main() -> Litestar:
    """Return the configured Litestar application."""
    return app


if __name__ == "__main__":
    print(main())
