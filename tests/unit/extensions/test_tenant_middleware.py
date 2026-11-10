"""Framework-free tenant extraction and scope ownership."""

import asyncio
from typing import Annotated, Any
from uuid import UUID

import pytest

from sqlspec.exceptions import ImproperConfigurationError, TenantContextError
from sqlspec.utils.tenant import (
    SQLSpecTenantMiddleware,
    TenantContext,
    TenantContextPayload,
    coerce_tenant_payload,
    single_membership_tenant_extractor,
)


@pytest.mark.parametrize(
    "value",
    ["a", UUID("12345678-1234-5678-1234-567812345678"), TenantContextPayload("a"), TenantContextPayload.bypass(), None],
)
def test_coerce_tenant_payload(value: Any) -> None:
    payload = coerce_tenant_payload(value)
    if value is None or isinstance(value, TenantContextPayload):
        assert payload is value
    else:
        assert payload == TenantContextPayload(value)


@pytest.mark.parametrize("value", [123, False, {}, "", "a\x00", "x" * 257])
def test_coerce_rejects_invalid_values(value: Any) -> None:
    with pytest.raises(TenantContextError):
        coerce_tenant_payload(value)


@pytest.mark.parametrize("memberships,expected", [([], None), (["a"], TenantContextPayload("a")), (["a", "b"], None)])
def test_single_membership(memberships: list[str], expected: TenantContextPayload | None) -> None:
    assert single_membership_tenant_extractor(lambda scope: memberships)({}) == expected


def test_middleware_validates_extractor() -> None:
    with pytest.raises(ImproperConfigurationError):
        SQLSpecTenantMiddleware(None, extractor=None)


@pytest.mark.anyio
@pytest.mark.parametrize("scope_type", ["http", "websocket"])
@pytest.mark.parametrize("value", ["a", TenantContextPayload.bypass(), None])
async def test_middleware_binds_and_restores(scope_type: str, value: Any) -> None:
    seen = []

    async def app(scope: Any, receive: Any, send: Any) -> None:
        seen.append(TenantContext.get())

    with TenantContext.scope("outer"):
        await SQLSpecTenantMiddleware(app, extractor=lambda scope: value)({"type": scope_type}, None, None)
        assert TenantContext.get_tenant_id() == "outer"
    assert seen == [coerce_tenant_payload(value)]


@pytest.mark.anyio
async def test_async_extractor_and_concurrent_invocations() -> None:
    seen = []

    async def extractor(scope: Any) -> str:
        await asyncio.sleep(0)
        return scope["tenant"]

    async def app(scope: Any, receive: Any, send: Any) -> None:
        await asyncio.sleep(0)
        seen.append((scope["tenant"], TenantContext.get_tenant_id()))

    middleware = SQLSpecTenantMiddleware(app, extractor=extractor)
    await asyncio.gather(*(middleware({"type": "http", "tenant": tenant}, None, None) for tenant in ("a", "b")))
    assert sorted(seen) == [("a", "a"), ("b", "b")]
    assert TenantContext.get() is None


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [ValueError("body"), asyncio.CancelledError()])
async def test_inner_error_restores_context(failure: BaseException) -> None:
    async def app(scope: Any, receive: Any, send: Any) -> None:
        raise failure

    with TenantContext.scope("outer"):
        with pytest.raises(type(failure)):
            await SQLSpecTenantMiddleware(app, extractor=lambda scope: "a")({"type": "http"}, None, None)
        assert TenantContext.get_tenant_id() == "outer"


@pytest.mark.anyio
async def test_lifespan_does_not_extract_or_change_context() -> None:
    seen = []

    def extractor(scope: Any) -> str:
        raise AssertionError("lifespan must not extract")

    async def app(scope: Any, receive: Any, send: Any) -> None:
        seen.append(TenantContext.get_tenant_id())

    with TenantContext.scope("outer"):
        await SQLSpecTenantMiddleware(app, extractor=extractor)({"type": "lifespan"}, None, None)
    assert seen == ["outer"]


@pytest.mark.anyio
@pytest.mark.parametrize("async_extractor", [False, True])
async def test_extractor_failure_prevents_application(async_extractor: bool) -> None:
    called = []

    def extractor(scope: Any) -> str:
        raise KeyError("extract")

    async def async_extract(scope: Any) -> str:
        return extractor(scope)

    async def app(scope: Any, receive: Any, send: Any) -> None:
        called.append(True)

    with TenantContext.scope("outer"):
        with pytest.raises(KeyError):
            await SQLSpecTenantMiddleware(app, extractor=async_extract if async_extractor else extractor)(
                {"type": "http"}, None, None
            )
        assert TenantContext.get_tenant_id() == "outer"
    assert called == []


def test_framework_exports_and_registration() -> None:
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route

    from sqlspec.extensions.fastapi import SQLSpecTenantMiddleware as FastAPIMiddleware
    from sqlspec.extensions.starlette import SQLSpecTenantMiddleware as StarletteMiddleware

    assert FastAPIMiddleware is StarletteMiddleware is SQLSpecTenantMiddleware

    async def endpoint(request: Any) -> Any:
        return PlainTextResponse(TenantContext.get_tenant_id())

    starlette = Starlette(routes=[Route("/", endpoint)])
    starlette.add_middleware(StarletteMiddleware, extractor=lambda scope: "a")
    with TestClient(starlette) as client:
        assert client.get("/").text == "a"

    fastapi = FastAPI()
    fastapi.add_middleware(FastAPIMiddleware, extractor=lambda scope: "b")

    @fastapi.get("/")
    async def route(tenant: Annotated[str, Depends(TenantContext.get_tenant_id)]) -> str:
        return tenant

    with TestClient(fastapi) as client:
        assert client.get("/").json() == "b"
