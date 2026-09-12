"""Tests for the Litestar plugin's NotFoundError and IntegrityError exception handlers."""

from typing import TYPE_CHECKING, Any

import pytest
from litestar import Router, get
from litestar.plugins.problem_details import ProblemDetailsConfig, ProblemDetailsPlugin
from litestar.response import Response
from litestar.testing import create_test_client

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.base import SQLSpec
from sqlspec.exceptions import IntegrityError, NotFoundError, SQLSpecError, UniqueViolationError
from sqlspec.extensions.litestar.plugin import SQLSpecPlugin

if TYPE_CHECKING:
    from litestar.types import ASGIApp, Message, Receive, Scope, Send

REPOSITORY_ERRORS = pytest.mark.parametrize(
    ("error", "status_code", "detail", "http_exception"),
    [
        (UniqueViolationError("duplicate key value violates users_email_key"), 409, "Conflict", "ClientException"),
        (NotFoundError("gone"), 404, "gone", "NotFoundException"),
    ],
    ids=["integrity", "not_found"],
)


def _build_plugin(*, correlation: bool = True) -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            extension_config={"litestar": {"enable_correlation_middleware": correlation}},
        )
    )
    return SQLSpecPlugin(sqlspec=sqlspec)


def _raising_route(error: Exception) -> Any:
    @get("/error")
    async def raise_error() -> None:
        raise error

    return raise_error


def _teapot_handler(_request: Any, exc: Exception) -> "Response[dict[str, str]]":
    return Response(content={"handled": type(exc).__name__}, status_code=418)


class _HeaderMiddleware:
    def __init__(self, app: "ASGIApp") -> None:
        self.app = app

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        async def send_with_header(message: "Message") -> None:
            if message["type"] == "http.response.start":
                message["headers"] = [*message.get("headers", []), (b"x-app-middleware", b"1")]
            await send(message)

        await self.app(scope, receive, send_with_header)


class _TaggedError(Exception):
    pass


class _TaggedUniqueViolationError(UniqueViolationError, _TaggedError):
    pass


@REPOSITORY_ERRORS
@pytest.mark.parametrize("correlation", [True, False], ids=["route_middleware", "no_route_middleware"])
def test_repository_error_status_and_body(
    error: Exception, status_code: int, detail: str, http_exception: str, correlation: bool
) -> None:
    with create_test_client(
        route_handlers=[_raising_route(error)], plugins=[_build_plugin(correlation=correlation)]
    ) as client:
        response = client.get("/error")

    assert response.status_code == status_code
    assert response.json() == {"status_code": status_code, "detail": detail}


@REPOSITORY_ERRORS
def test_repository_error_response_keeps_app_middleware_headers(
    error: Exception, status_code: int, detail: str, http_exception: str
) -> None:
    with create_test_client(
        route_handlers=[_raising_route(error)], plugins=[_build_plugin()], middleware=[_HeaderMiddleware]
    ) as client:
        response = client.get("/error")

    assert response.status_code == status_code
    assert response.headers.get("x-app-middleware") == "1"


@REPOSITORY_ERRORS
def test_repository_error_runs_after_exception_hooks_once(
    error: Exception, status_code: int, detail: str, http_exception: str
) -> None:
    seen: list[str] = []

    async def record(exc: BaseException, _scope: "Scope") -> None:
        seen.append(type(exc).__name__)

    with create_test_client(
        route_handlers=[_raising_route(error)], plugins=[_build_plugin()], after_exception=[record]
    ) as client:
        response = client.get("/error")

    assert response.status_code == status_code
    assert seen == [type(error).__name__]


@REPOSITORY_ERRORS
def test_repository_error_uses_status_code_handler(
    error: Exception, status_code: int, detail: str, http_exception: str
) -> None:
    with create_test_client(
        route_handlers=[_raising_route(error)],
        plugins=[_build_plugin()],
        exception_handlers={status_code: _teapot_handler},
    ) as client:
        response = client.get("/error")

    assert response.status_code == 418
    assert response.json() == {"handled": http_exception}


@REPOSITORY_ERRORS
def test_repository_error_renders_problem_details(
    error: Exception, status_code: int, detail: str, http_exception: str
) -> None:
    problem_details = ProblemDetailsPlugin(ProblemDetailsConfig(enable_for_all_http_exceptions=True))
    with create_test_client(
        route_handlers=[_raising_route(error)], plugins=[_build_plugin(), problem_details]
    ) as client:
        response = client.get("/error")

    assert response.status_code == status_code
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["title"] == detail


@pytest.mark.parametrize(
    ("handler_key", "error"),
    [
        (IntegrityError, UniqueViolationError("dup")),
        (UniqueViolationError, UniqueViolationError("dup")),
        (NotFoundError, NotFoundError("gone")),
    ],
    ids=["integrity_error", "integrity_subclass", "not_found_error"],
)
def test_user_handler_wins(handler_key: "type[Exception]", error: Exception) -> None:
    with create_test_client(
        route_handlers=[_raising_route(error)],
        plugins=[_build_plugin()],
        exception_handlers={handler_key: _teapot_handler},
    ) as client:
        response = client.get("/error")

    assert response.status_code == 418
    assert response.json() == {"handled": type(error).__name__}


@pytest.mark.parametrize(
    ("handler_key", "error"),
    [
        (500, UniqueViolationError("dup")),
        (SQLSpecError, UniqueViolationError("dup")),
        (Exception, UniqueViolationError("dup")),
        (_TaggedError, _TaggedUniqueViolationError("dup")),
    ],
    ids=["status_500", "sqlspec_error", "exception", "subclass_mixin"],
)
def test_broader_app_handler_takes_precedence_over_integrity_default(
    handler_key: "int | type[Exception]", error: Exception
) -> None:
    with create_test_client(
        route_handlers=[_raising_route(error)],
        plugins=[_build_plugin()],
        exception_handlers={handler_key: _teapot_handler},
    ) as client:
        response = client.get("/error")

    assert response.status_code == 418
    assert response.json() == {"handled": type(error).__name__}


def test_broader_router_handler_takes_precedence_over_integrity_default() -> None:
    router = Router(
        "/api",
        route_handlers=[_raising_route(UniqueViolationError("dup"))],
        exception_handlers={SQLSpecError: _teapot_handler},
    )
    with create_test_client(route_handlers=[router], plugins=[_build_plugin()]) as client:
        response = client.get("/api/error")

    assert response.status_code == 418
    assert response.json() == {"handled": "UniqueViolationError"}
