"""Litestar server-side sessions backed by SQL Server."""

from litestar import Litestar, get
from litestar.connection import Request
from litestar.middleware.session.server_side import ServerSideSessionConfig

from sqlspec.adapters.mssql_python import MssqlPythonAsyncConfig
from sqlspec.adapters.mssql_python.litestar import MssqlPythonStore

config = MssqlPythonAsyncConfig(
    connection_config={
        "server": "localhost,1433",
        "database": "app",
        "uid": "sa",
        "pwd": "change-me",
        "trust_server_certificate": True,
    },
    extension_config={"litestar": {"session_table": "litestar_session"}},
)

session_store = MssqlPythonStore(config)
session_config = ServerSideSessionConfig(store="sessions", renew_on_access=True)


@get("/session")
async def read_session(request: Request) -> dict[str, object]:
    """Read and update a counter in the server-side session."""
    visits = int(request.session.get("visits", 0)) + 1
    request.session["visits"] = visits
    return {"visits": visits}


app = Litestar(
    route_handlers=[read_session],
    middleware=[session_config.middleware],
    stores={"sessions": session_store},
    on_startup=[session_store.create_table],
)
