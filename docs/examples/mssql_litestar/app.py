"""Minimal Litestar app using the mssql-python async driver."""

from litestar import Litestar, get
from litestar.params import FromPath

from sqlspec import SQLSpec
from sqlspec.adapters.mssql_python import MssqlPythonAsyncConfig, MssqlPythonAsyncDriver
from sqlspec.extensions.litestar import SQLSpecPlugin

config = MssqlPythonAsyncConfig(
    connection_config={
        "server": "localhost,1433",
        "database": "app",
        "uid": "sa",
        "pwd": "change-me",
        "trust_server_certificate": True,
    },
    extension_config={"litestar": {"session_key": "db_session"}},
)

sqlspec = SQLSpec()
sqlspec.add_config(config)


@get("/users/{user_id:int}")
async def get_user(user_id: FromPath[int], db_session: MssqlPythonAsyncDriver) -> dict[str, object]:
    """Return a user by ID."""
    row = await db_session.select_one_or_none("SELECT id, name FROM users WHERE id = ?", (user_id,))
    return {"user": dict(row) if row else None}


app = Litestar(route_handlers=[get_user], plugins=[SQLSpecPlugin(sqlspec=sqlspec)])
