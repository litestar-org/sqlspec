"""Verified tenant resolution and guards for the Litestar integration."""

from collections.abc import Awaitable
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("litestar")

from sqlspec.extensions.litestar.tenant import TenantResolutionSettings, resolve_litestar_tenant, set_connection_tenant
from sqlspec.utils.tenant import TenantContextPayload


async def _resolve(scope: dict[str, Any], **settings: Any) -> TenantContextPayload | None:
    value = resolve_litestar_tenant(scope, TenantResolutionSettings(**settings))
    return await value if isinstance(value, Awaitable) else value


async def test_tenant_bypass_precedes_extractor() -> None:
    def extractor(scope: object) -> None:
        raise AssertionError("bypass must skip extractor")

    result = await _resolve({"route_handler": SimpleNamespace(opt={"tenant_bypass": True})}, tenant_extractor=extractor)
    assert result == TenantContextPayload.bypass()


@pytest.mark.parametrize("as_connection", [False, True])
async def test_tenant_explicit_scope_receipt(as_connection: bool) -> None:
    scope: dict[str, Any] = {}
    target = SimpleNamespace(scope=scope) if as_connection else scope
    payload = set_connection_tenant(target, UUID("12345678-1234-5678-1234-567812345678"), variables={"region": "eu"})

    assert await _resolve(scope) == payload
    assert payload.tenant_id == "12345678-1234-5678-1234-567812345678"
    assert dict(payload.variables) == {"region": "eu"}


@pytest.mark.parametrize(
    "value", ["acme", UUID("12345678-1234-5678-1234-567812345678"), None, TenantContextPayload.bypass()]
)
@pytest.mark.parametrize("is_async", [False, True])
async def test_tenant_custom_extractor(value: object, is_async: bool) -> None:
    async def async_extractor(scope: object) -> object:
        return value

    def sync_extractor(scope: object) -> object:
        return value

    result = await _resolve({}, tenant_extractor=async_extractor if is_async else sync_extractor)
    expected = value if isinstance(value, TenantContextPayload) or value is None else TenantContextPayload(value)
    assert result == expected


@pytest.mark.parametrize("scope", [{}, {"user": SimpleNamespace(is_authenticated=False, user={"tenant_id": "acme"})}])
async def test_tenant_no_verified_authentication(scope: dict[str, Any]) -> None:
    assert await _resolve(scope) is None


@pytest.mark.parametrize(
    "scope",
    [
        {"user": {"tenant_id": "acme"}},
        {"user": SimpleNamespace(tenant_id="acme")},
        {"auth": SimpleNamespace(authorization=SimpleNamespace(attributes={"tenant_id": "acme"}))},
        {"auth": SimpleNamespace(extras={"tenant_id": "acme"})},
        {"auth": {"tenant_id": "acme"}},
        {"user": SimpleNamespace(is_authenticated=True, user={"tenant_id": "acme"})},
    ],
)
async def test_tenant_direct_verified_identity(scope: dict[str, Any]) -> None:
    assert await _resolve(scope) == TenantContextPayload("acme")


@pytest.mark.parametrize("membership_source", ["snapshot_ids", "snapshot_roles", "user", "auth_extras"])
async def test_tenant_single_membership_sources(membership_source: str) -> None:
    scope: dict[str, Any]
    if membership_source == "snapshot_ids":
        scope = {"auth": SimpleNamespace(authorization=SimpleNamespace(tenant_ids=frozenset({"acme"})))}
    elif membership_source == "snapshot_roles":
        scope = {"auth": SimpleNamespace(authorization=SimpleNamespace(tenant_roles={"acme": frozenset({"reader"})}))}
    elif membership_source == "user":
        scope = {"user": {"tenant_ids": ["acme"]}}
    else:
        scope = {"auth": SimpleNamespace(extras={"tenant_ids": ["acme"]})}
    assert await _resolve(scope) == TenantContextPayload("acme")


async def test_tenant_multiple_memberships_need_selection() -> None:
    assert await _resolve({"user": {"tenant_ids": ["acme", "globex"]}}) is None


@pytest.mark.parametrize("selected", ["acme", "evil"])
@pytest.mark.parametrize("selector", ["path", "session", "header", "guard"])
async def test_tenant_selectors_require_verified_membership(selected: str, selector: str) -> None:
    scope: dict[str, Any] = {"user": {"tenant_id": "acme"}}
    if selector == "path":
        scope["path_params"] = {"tenant_id": selected}
    elif selector == "session":
        scope["session"] = {"tenant_id": selected}
    elif selector == "header":
        scope["headers"] = [(b"x-tenant-id", selected.encode())]
    else:
        tenant_guard = SimpleNamespace(tenant_parameter="org_id")
        composite = SimpleNamespace(children=[SimpleNamespace(role="reader"), tenant_guard])
        scope["route_handler"] = SimpleNamespace(opt={}, resolve_guards=lambda: [composite])
        scope["path_params"] = {"org_id": selected}
    result = await _resolve(scope, header="x-tenant-id" if selector == "header" else None)
    assert result == (TenantContextPayload("acme") if selected == "acme" else None)


async def test_tenant_session_selector_precedes_header() -> None:
    result = await _resolve(
        {
            "user": {"tenant_ids": ["acme", "globex"]},
            "session": {"tenant_id": "globex"},
            "headers": [(b"x-tenant-id", b"acme")],
        },
        header="x-tenant-id",
    )
    assert result == TenantContextPayload("globex")


async def test_tenant_header_disabled_and_variables_preserved() -> None:
    result = await _resolve({
        "user": {"tenant_id": "acme", "tenant_variables": {"region": "eu"}},
        "headers": [(b"x-tenant-id", b"evil")],
    })
    assert result == TenantContextPayload("acme", variables={"region": "eu"})


async def test_tenant_snapshot_variables_and_membership_union() -> None:
    result = await _resolve({
        "user": {"tenant_ids": ["globex"]},
        "auth": SimpleNamespace(
            authorization=SimpleNamespace(tenant_ids={"acme"}, attributes={"tenant_variables": {"region": "eu"}})
        ),
        "path_params": {"tenant_id": "acme"},
    })
    assert result == TenantContextPayload("acme", variables={"region": "eu"})


@pytest.mark.parametrize("scope_type", ["http", "websocket"])
@pytest.mark.parametrize("failure", ["unauthenticated", "unauthorized"])
async def test_tenant_guard_rejects_unverified_selection(scope_type: str, failure: str) -> None:
    from litestar.exceptions import NotAuthorizedException, PermissionDeniedException, WebSocketException

    from sqlspec.extensions.litestar.tenant import require_tenant

    scope: dict[str, Any] = {"type": scope_type}
    if failure == "unauthorized":
        scope.update(user={"tenant_ids": ["acme"]}, path_params={"org_id": "evil"})
    guard = require_tenant(path_parameter="org_id")
    exception_type = (
        WebSocketException
        if scope_type == "websocket"
        else (NotAuthorizedException if failure == "unauthenticated" else PermissionDeniedException)
    )
    with pytest.raises(exception_type) as raised:
        await guard(SimpleNamespace(scope=scope), SimpleNamespace(opt={}))
    if scope_type == "websocket":
        assert raised.value.code == (4001 if failure == "unauthenticated" else 4003)
    else:
        assert raised.value.status_code == (401 if failure == "unauthenticated" else 403)


async def test_tenant_guard_authorizes_configured_membership_and_binds_receipt() -> None:
    from sqlspec.extensions.litestar.tenant import require_tenant

    scope: dict[str, Any] = {
        "type": "http",
        "user": {"org_ids": ["acme", "globex"], "variables": {"region": "eu"}},
        "headers": [(b"x-org-id", b"globex")],
    }
    guard = require_tenant(header="x-org-id", memberships_attr="org_ids", variables_attr="variables")
    await guard(SimpleNamespace(scope=scope), SimpleNamespace(opt={}))
    assert await _resolve(scope) == TenantContextPayload("globex", variables={"region": "eu"})


def test_tenant_litestar_public_exports() -> None:
    from sqlspec.extensions import litestar
    from sqlspec.utils.tenant import SQLSpecTenantMiddleware

    assert litestar.TenantResolutionSettings is TenantResolutionSettings
    assert litestar.set_connection_tenant is set_connection_tenant
    assert litestar.resolve_litestar_tenant is resolve_litestar_tenant
    assert litestar.SQLSpecTenantMiddleware is SQLSpecTenantMiddleware
    assert callable(litestar.require_tenant())


@pytest.mark.parametrize("trusted_tenant", ["acme", None])
def test_tenant_middleware_native_litestar_registration(trusted_tenant: str | None) -> None:
    from litestar import Litestar, get
    from litestar.middleware import DefineMiddleware
    from litestar.testing import TestClient

    from sqlspec.extensions.litestar import SQLSpecTenantMiddleware
    from sqlspec.utils.tenant import TenantContext

    @get("/")
    async def handler() -> dict[str, str | None]:
        return {"tenant": TenantContext.get_tenant_id()}

    app = Litestar(
        [handler], middleware=[DefineMiddleware(SQLSpecTenantMiddleware, extractor=lambda scope: trusted_tenant)]
    )
    with TestClient(app) as client:
        assert client.get("/", headers={"x-tenant-id": "evil"}).json() == {"tenant": trusted_tenant}
