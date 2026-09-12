"""Test handlers for SQLSpec Litestar extension."""

from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar import Router, get
from litestar.constants import HTTP_RESPONSE_START
from litestar.exceptions import HTTPException
from litestar.plugins.problem_details import ProblemDetailsConfig, ProblemDetailsPlugin
from litestar.response import Response
from litestar.testing import create_test_client

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.sqlite.config import SqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import (
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    NotFoundError,
    RepositoryError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.extensions.litestar import get_sqlspec_scope_state, set_sqlspec_scope_state
from sqlspec.extensions.litestar.handlers import (
    autocommit_handler_maker,
    connection_provider_maker,
    lifespan_handler_maker,
    manual_handler_maker,
    pool_provider_maker,
    session_provider_maker,
)
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin

if TYPE_CHECKING:
    from litestar.types import ASGIApp, Message, Receive, Scope, Send

pytestmark = pytest.mark.anyio


def test_session_provider_documents_mypyc_blockers() -> None:
    """The session provider should document why handlers.py stays interpreted."""
    source = Path("sqlspec/extensions/litestar/handlers.py").read_text()

    assert "*args/**kwargs" in source
    assert "yield in async def" in source
    assert "@contextlib.asynccontextmanager" in source
    assert "__signature__/__annotations__" in source


async def test_async_manual_handler_closes_connection() -> None:
    """Test async manual handler closes connection on terminus event."""
    connection_key = "test_connection"
    handler = manual_handler_maker(connection_key)

    mock_connection = AsyncMock()
    mock_connection.close = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    mock_connection.close.assert_awaited_once()
    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_async_manual_handler_ignores_non_terminus_events() -> None:
    """Test async manual handler ignores non-terminus events."""
    connection_key = "test_connection"
    handler = manual_handler_maker(connection_key)

    mock_connection = AsyncMock()
    mock_connection.close = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": "http.request"})

    await handler(message, scope)

    mock_connection.close.assert_not_awaited()
    assert get_sqlspec_scope_state(scope, connection_key) is mock_connection


async def test_async_autocommit_handler_commits_on_success() -> None:
    """Test async autocommit handler commits on 2xx status."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    mock_connection = AsyncMock()
    mock_connection.commit = AsyncMock()
    mock_connection.rollback = AsyncMock()
    mock_connection.close = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    mock_connection.commit.assert_awaited_once()
    mock_connection.rollback.assert_not_awaited()
    mock_connection.close.assert_awaited_once()


async def test_async_autocommit_handler_rolls_back_on_error() -> None:
    """Test async autocommit handler rolls back on 4xx/5xx status."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    mock_connection = AsyncMock()
    mock_connection.commit = AsyncMock()
    mock_connection.rollback = AsyncMock()
    mock_connection.close = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 500})

    await handler(message, scope)

    mock_connection.commit.assert_not_awaited()
    mock_connection.rollback.assert_awaited_once()
    mock_connection.close.assert_awaited_once()


async def test_async_autocommit_handler_with_redirect_commit() -> None:
    """Test async autocommit handler commits on 3xx when enabled."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key, commit_on_redirect=True)

    mock_connection = AsyncMock()
    mock_connection.commit = AsyncMock()
    mock_connection.rollback = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 301})

    await handler(message, scope)

    mock_connection.commit.assert_awaited_once()
    mock_connection.rollback.assert_not_awaited()


async def test_async_autocommit_handler_extra_commit_statuses() -> None:
    """Test async autocommit handler uses extra commit statuses."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key, extra_commit_statuses={418})

    mock_connection = AsyncMock()
    mock_connection.commit = AsyncMock()
    mock_connection.rollback = AsyncMock()

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 418})

    await handler(message, scope)

    mock_connection.commit.assert_awaited_once()
    mock_connection.rollback.assert_not_awaited()


async def test_async_autocommit_handler_raises_on_conflicting_statuses() -> None:
    """Test async autocommit handler raises error when status sets overlap."""
    with pytest.raises(ImproperConfigurationError) as exc_info:
        autocommit_handler_maker("test", extra_commit_statuses={418}, extra_rollback_statuses={418})

    assert "must not share" in str(exc_info.value)


async def test_async_lifespan_handler_creates_and_closes_pool() -> None:
    """Test async lifespan handler manages pool lifecycle."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"

    handler = lifespan_handler_maker(config, pool_key)

    mock_app = MagicMock()
    mock_app.state = {}
    mock_app.logger = None

    async with handler(mock_app):
        assert pool_key in mock_app.state
        pool = mock_app.state[pool_key]
        assert pool is not None

    assert pool_key not in mock_app.state


async def test_async_pool_provider_returns_pool() -> None:
    """Test async pool provider returns pool from state."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"

    provider = pool_provider_maker(config, pool_key)

    mock_pool = MagicMock()
    state = MagicMock()
    state.get.return_value = mock_pool
    scope = cast("Scope", {})

    result: Any = await provider(state, scope)

    assert result is mock_pool
    state.get.assert_called_once_with(pool_key)


async def test_async_pool_provider_raises_when_pool_missing() -> None:
    """Test async pool provider raises error when pool not in state."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"

    provider = pool_provider_maker(config, pool_key)

    state = MagicMock()
    state.get.return_value = None
    scope = cast("Scope", {})

    with pytest.raises(ImproperConfigurationError) as exc_info:
        await provider(state, scope)

    assert pool_key in str(exc_info.value)
    assert "not found in application state" in str(exc_info.value)


async def test_async_connection_provider_creates_connection() -> None:
    """Test async connection provider creates connection from pool."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"
    connection_key = "test_connection"

    provider = connection_provider_maker(config, pool_key, connection_key)

    mock_pool = await config.create_pool()
    state = MagicMock()
    state.get.return_value = mock_pool
    scope = cast("Scope", {})

    connection: Any
    async for connection in provider(state, scope):
        assert connection is not None
        assert get_sqlspec_scope_state(scope, connection_key) is connection


async def test_async_connection_provider_raises_when_pool_missing() -> None:
    """Test async connection provider raises error when pool missing."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"
    connection_key = "test_connection"

    provider = connection_provider_maker(config, pool_key, connection_key)

    state = MagicMock()
    state.get.return_value = None
    scope = cast("Scope", {})

    with pytest.raises(ImproperConfigurationError) as exc_info:
        async for _ in provider(state, scope):
            pass

    assert pool_key in str(exc_info.value)


async def test_sync_connection_provider_supports_context_manager() -> None:
    """Test sync connection provider wraps sync context managers."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"
    connection_key = "test_connection"

    provider = connection_provider_maker(config, pool_key, connection_key)

    pool = config.create_pool()
    state = MagicMock()
    state.get.return_value = pool
    scope = cast("Scope", {})

    try:
        async for connection in provider(state, scope):
            assert connection is not None
            assert get_sqlspec_scope_state(scope, connection_key) is connection
    finally:
        pool.close()

    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_async_session_provider_creates_session() -> None:
    """Test async session provider creates driver session."""
    config = AiosqliteConfig(connection_config={"database": ":memory:"})
    connection_key = "test_connection"

    provider = session_provider_maker(config, connection_key)

    mock_connection = AsyncMock()

    session: Any
    async for session in provider(mock_connection):
        assert session is not None
        assert session.connection is mock_connection


def test_handlers_use_ensure_async_unconditionally() -> None:
    """Test that unified handlers normalize sync and async callables via ensure_async_."""
    from pathlib import Path

    from sqlspec.extensions.litestar import handlers

    source = handlers.__file__
    assert source is not None

    content = Path(source).read_text()

    assert "from sqlspec.utils.sync_tools import ensure_async_" in content
    assert "is_async" not in content, "handlers should not branch on is_async"
    assert "await ensure_async_(connection.close)()" in content, "close should go through ensure_async_"
    assert "await ensure_async_(connection.commit)()" in content, "commit should go through ensure_async_"
    assert "await ensure_async_(connection.rollback)()" in content, "rollback should go through ensure_async_"


async def test_sync_manual_handler_closes_connection() -> None:
    """Test manual handler closes sync connections through ensure_async_."""
    connection_key = "test_connection"
    handler = manual_handler_maker(connection_key)

    mock_connection = MagicMock()
    mock_connection.close = MagicMock(return_value=None)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    mock_connection.close.assert_called_once()
    assert get_sqlspec_scope_state(scope, connection_key) is None


async def test_sync_autocommit_handler_commits_on_success() -> None:
    """Test autocommit handler commits sync connections through ensure_async_."""
    connection_key = "test_connection"
    handler = autocommit_handler_maker(connection_key)

    mock_connection = MagicMock()
    mock_connection.commit = MagicMock(return_value=None)
    mock_connection.rollback = MagicMock(return_value=None)
    mock_connection.close = MagicMock(return_value=None)

    scope = cast("Scope", {})
    set_sqlspec_scope_state(scope, connection_key, mock_connection)

    message = cast("Message", {"type": HTTP_RESPONSE_START, "status": 200})

    await handler(message, scope)

    mock_connection.commit.assert_called_once()
    mock_connection.rollback.assert_not_called()
    mock_connection.close.assert_called_once()


async def test_sync_lifespan_handler_creates_and_closes_pool() -> None:
    """Test lifespan handler manages a sync config's pool lifecycle."""
    config = SqliteConfig(connection_config={"database": ":memory:"})
    pool_key = "test_pool"

    handler = lifespan_handler_maker(config, pool_key)

    mock_app = MagicMock()
    mock_app.state = {}
    mock_app.logger = None

    async with handler(mock_app):
        assert pool_key in mock_app.state
        assert mock_app.state[pool_key] is not None

    assert pool_key not in mock_app.state


def _build_integrity_plugin(*, correlation: bool = True) -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"enable_correlation_middleware": correlation}},
        )
    )
    return SQLSpecPlugin(sqlspec=sqlspec)


class _HeaderMiddleware:
    def __init__(self, app: "ASGIApp") -> None:
        self.app = app

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        async def send_with_header(message: "Message") -> None:
            if message["type"] == "http.response.start":
                message["headers"] = [*message.get("headers", []), (b"x-app-middleware", b"1")]
            await send(message)

        await self.app(scope, receive, send_with_header)


def _teapot_handler(_request: Any, exc: Exception) -> "Response[dict[str, str]]":
    return Response(content={"handled": type(exc).__name__}, status_code=418)


_REPOSITORY_ERRORS = pytest.mark.parametrize(
    ("error", "status_code", "detail"),
    [(UniqueViolationError("dup"), 409, "Conflict"), (NotFoundError("gone"), 404, "gone")],
    ids=["integrity", "not_found"],
)


def _raising_route(error: Exception) -> Any:
    @get("/error")
    async def raise_error() -> None:
        raise error

    return raise_error


@pytest.mark.parametrize("error_type", [UniqueViolationError, ForeignKeyViolationError])
def test_integrity_error_maps_to_409(error_type: "type[IntegrityError]") -> None:
    """A route raising an IntegrityError subclass returns 409 without exposing the exception message."""

    @get("/conflict")
    async def raise_conflict() -> None:
        raise error_type("dup")

    with create_test_client(route_handlers=[raise_conflict], plugins=[_build_integrity_plugin()]) as client:
        response = client.get("/conflict")

    assert response.status_code == 409
    assert response.json()["detail"] == "Conflict"
    assert "dup" not in response.text


def test_user_integrity_handler_wins() -> None:
    """A user-registered IntegrityError handler replaces the plugin default."""

    def user_handler(_request: Any, exc: IntegrityError) -> "Response[dict[str, str]]":
        return Response(content={"custom": str(exc)}, status_code=422)

    @get("/conflict")
    async def raise_conflict() -> None:
        raise UniqueViolationError("dup")

    with create_test_client(
        route_handlers=[raise_conflict],
        plugins=[_build_integrity_plugin()],
        exception_handlers={IntegrityError: user_handler},
    ) as client:
        response = client.get("/conflict")

    assert response.status_code == 422
    assert response.json() == {"custom": "dup"}


@_REPOSITORY_ERRORS
@pytest.mark.parametrize("correlation", [True, False], ids=["route_middleware", "no_route_middleware"])
def test_repository_error_response_with_and_without_route_middleware(
    error: Exception, status_code: int, detail: str, correlation: bool
) -> None:
    """Repository errors render their HTTP status whether or not the route has middleware."""
    with create_test_client(
        route_handlers=[_raising_route(error)], plugins=[_build_integrity_plugin(correlation=correlation)]
    ) as client:
        response = client.get("/error")

    assert response.status_code == status_code
    assert response.json()["detail"] == detail


@_REPOSITORY_ERRORS
def test_repository_error_response_passes_through_app_middleware(
    error: Exception, status_code: int, detail: str
) -> None:
    """Headers set by application middleware are present on the rendered error response."""
    with create_test_client(
        route_handlers=[_raising_route(error)], plugins=[_build_integrity_plugin()], middleware=[_HeaderMiddleware]
    ) as client:
        response = client.get("/error")

    assert response.status_code == status_code
    assert response.headers.get("x-app-middleware") == "1"


@_REPOSITORY_ERRORS
def test_repository_error_runs_after_exception_hooks_once(error: Exception, status_code: int, detail: str) -> None:
    """after_exception hooks observe the original exception exactly once."""
    seen: list[str] = []

    async def record(exc: BaseException, _scope: "Scope") -> None:
        seen.append(type(exc).__name__)

    with create_test_client(
        route_handlers=[_raising_route(error)], plugins=[_build_integrity_plugin()], after_exception=[record]
    ) as client:
        response = client.get("/error")

    assert response.status_code == status_code
    assert seen == [type(error).__name__]


@_REPOSITORY_ERRORS
def test_repository_error_uses_status_code_handler(error: Exception, status_code: int, detail: str) -> None:
    """An application handler for the mapped status code renders the response."""
    with create_test_client(
        route_handlers=[_raising_route(error)],
        plugins=[_build_integrity_plugin()],
        exception_handlers={status_code: _teapot_handler},
    ) as client:
        response = client.get("/error")

    assert response.status_code == 418
    assert response.json() == {"handled": "NotFoundException" if status_code == 404 else "ClientException"}


@_REPOSITORY_ERRORS
@pytest.mark.parametrize("correlation", [True, False], ids=["route_middleware", "no_route_middleware"])
def test_repository_error_renders_problem_details(
    error: Exception, status_code: int, detail: str, correlation: bool
) -> None:
    """The problem details plugin renders the mapped HTTP exception."""
    problem_details = ProblemDetailsPlugin(ProblemDetailsConfig(enable_for_all_http_exceptions=True))
    with create_test_client(
        route_handlers=[_raising_route(error)],
        plugins=[_build_integrity_plugin(correlation=correlation), problem_details],
    ) as client:
        response = client.get("/error")

    assert response.status_code == status_code
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["title"] == detail


@pytest.mark.parametrize("handler_key", [500, SQLSpecError, RepositoryError, Exception])
def test_broader_app_handler_takes_precedence_over_integrity_default(handler_key: "int | type[Exception]") -> None:
    """Application handlers for a status 500 or a base class of IntegrityError receive the original exception."""
    with create_test_client(
        route_handlers=[_raising_route(UniqueViolationError("dup"))],
        plugins=[_build_integrity_plugin()],
        exception_handlers={handler_key: _teapot_handler},
    ) as client:
        response = client.get("/error")

    assert response.status_code == 418
    assert response.json() == {"handled": "UniqueViolationError"}


def test_broader_router_handler_takes_precedence_over_integrity_default() -> None:
    """Router-level handlers for a base class of IntegrityError receive the original exception."""
    router = Router(
        "/api",
        route_handlers=[_raising_route(UniqueViolationError("dup"))],
        exception_handlers={SQLSpecError: _teapot_handler},
    )
    with create_test_client(route_handlers=[router], plugins=[_build_integrity_plugin()]) as client:
        response = client.get("/api/error")

    assert response.status_code == 418
    assert response.json() == {"handled": "UniqueViolationError"}


def test_integrity_subclass_handler_takes_precedence() -> None:
    """A handler for a specific IntegrityError subclass renders that subclass."""
    with create_test_client(
        route_handlers=[_raising_route(UniqueViolationError("dup"))],
        plugins=[_build_integrity_plugin()],
        exception_handlers={UniqueViolationError: _teapot_handler, HTTPException: _teapot_handler},
    ) as client:
        response = client.get("/error")

    assert response.status_code == 418
    assert response.json() == {"handled": "UniqueViolationError"}
