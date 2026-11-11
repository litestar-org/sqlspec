"""Verified tenant resolution and guards for the Litestar integration."""

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import asynccontextmanager, contextmanager
from inspect import isawaitable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import pytest

pytest.importorskip("litestar")

from litestar import Controller, Litestar, WebSocket, get, websocket
from litestar.connection import ASGIConnection
from litestar.di import NamedDependency
from litestar.exceptions import (
    NotAuthorizedException,
    PermissionDeniedException,
    WebSocketDisconnect,
    WebSocketException,
)
from litestar.middleware import DefineMiddleware
from litestar.middleware.authentication import AbstractAuthenticationMiddleware, AuthenticationResult
from litestar.params import FromPath
from litestar.testing import TestClient
from litestar.types import ASGIApp, Empty, Guard

from sqlspec import SQLSpec
from sqlspec.adapters.aiosqlite import AiosqliteConfig
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.exceptions import ImproperConfigurationError, TenantContextError
from sqlspec.extensions import litestar as litestar_ext
from sqlspec.extensions.litestar import (
    SQLSpecPlugin,
    SQLSpecTenantMiddleware,
    TenantResolutionSettings,
    is_scope_authenticated,
    require_tenant,
    resolve_litestar_tenant,
    set_connection_tenant,
)
from sqlspec.utils.tenant import TenantContext, TenantContextPayload

if TYPE_CHECKING:
    from litestar.handlers import BaseRouteHandler


async def _resolve(scope: dict[str, Any], **settings: Any) -> TenantContextPayload | None:
    value = resolve_litestar_tenant(scope, TenantResolutionSettings(**settings))
    return await value if isinstance(value, Awaitable) else value


async def _invoke_guard(
    guard: Guard, scope: dict[str, Any], *, app: Any = None, opt: dict[str, Any] | None = None
) -> None:
    connection = cast("ASGIConnection[Any, Any, Any, Any]", SimpleNamespace(scope=scope, app=app))
    route_handler = cast("BaseRouteHandler", SimpleNamespace(opt=opt or {}))
    result = guard(connection, route_handler)
    if isawaitable(result):
        await result


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
async def test_tenant_custom_extractor(value: TenantContextPayload | str | UUID | None, is_async: bool) -> None:
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
@pytest.mark.parametrize("selector", ["path", "session", "header", "guard", "wrapped_guard"])
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

        def wrapped_guard() -> None:
            pass

        setattr(wrapped_guard, "children", composite.children)
        route_guard = SimpleNamespace(func=wrapped_guard) if selector == "wrapped_guard" else composite
        scope["route_handler"] = SimpleNamespace(opt={}, resolve_guards=lambda: [route_guard])
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
        await _invoke_guard(guard, scope)
    if isinstance(raised.value, WebSocketException):
        assert raised.value.code == (4001 if failure == "unauthenticated" else 4003)
    else:
        assert raised.value.status_code == (401 if failure == "unauthenticated" else 403)


async def test_tenant_guard_authorizes_configured_membership_and_binds_receipt() -> None:
    scope: dict[str, Any] = {
        "type": "http",
        "user": {"org_ids": ["acme", "globex"], "variables": {"region": "eu"}},
        "headers": [(b"x-org-id", b"globex")],
    }
    guard = require_tenant(header="x-org-id", memberships_attr="org_ids", variables_attr="variables")
    await _invoke_guard(guard, scope)
    assert await _resolve(scope) == TenantContextPayload("globex", variables={"region": "eu"})


def test_tenant_litestar_public_exports() -> None:
    assert litestar_ext.TenantResolutionSettings is TenantResolutionSettings
    assert litestar_ext.set_connection_tenant is set_connection_tenant
    assert litestar_ext.resolve_litestar_tenant is resolve_litestar_tenant
    assert litestar_ext.is_scope_authenticated is is_scope_authenticated
    assert litestar_ext.SQLSpecTenantMiddleware is SQLSpecTenantMiddleware
    assert callable(litestar_ext.require_tenant())


@pytest.mark.parametrize("trusted_tenant", ["acme", None])
def test_tenant_middleware_native_litestar_registration(trusted_tenant: str | None) -> None:
    @get("/")
    async def handler() -> dict[str, str | None]:
        return {"tenant": TenantContext.get_tenant_id()}

    app = Litestar(
        [handler],
        openapi_config=None,
        middleware=[
            DefineMiddleware(
                cast("Callable[..., ASGIApp]", SQLSpecTenantMiddleware), extractor=lambda scope: trusted_tenant
            )
        ],
    )
    with TestClient(app) as client:
        assert client.get("/", headers={"x-tenant-id": "evil"}).json() == {"tenant": trusted_tenant}


def test_sqlspec_plugin_direct_configs_get_config_and_immediate_app_state() -> None:
    auth_cfg = AiosqliteConfig(
        bind_key="auth",
        connection_config={"database": ":memory:"},
        extension_config={
            "litestar": {"session_key": "auth_session", "connection_key": "auth_conn", "pool_key": "auth_pool"}
        },
    )
    tenant_cfg = AsyncpgConfig(
        bind_key="tenant",
        connection_config={"dsn": "postgresql://localhost/tenant"},
        driver_features={"tenant_strategy": True},
        extension_config={"litestar": {"tenant_header": "x-org-id", "tenant_path_parameter": "org_id"}},
    )

    single = SQLSpecPlugin(auth_cfg)
    assert isinstance(single.sqlspec, SQLSpec)
    assert single.get_config() is auth_cfg
    assert single.get_config(AiosqliteConfig) is auth_cfg
    assert single.get_config(auth_cfg) is auth_cfg
    assert single.get_config("auth") is auth_cfg

    app = Litestar([], openapi_config=None, plugins=[single])
    assert app.state.sqlspec is single
    assert single.get_config("auth_session") is auth_cfg

    multi = SQLSpecPlugin([auth_cfg, tenant_cfg])
    assert multi.get_config(AiosqliteConfig) is auth_cfg
    assert multi.get_config(AsyncpgConfig) is tenant_cfg
    assert multi.get_config("tenant") is tenant_cfg
    assert multi.resolve_tenant_settings() == TenantResolutionSettings(path_parameter="org_id", header="x-org-id")
    with pytest.raises(KeyError, match="Multiple database configurations"):
        multi.get_config()

    empty = SQLSpecPlugin(SQLSpec())
    with pytest.raises(KeyError, match="No database configurations"):
        empty.get_config()


async def test_require_tenant_inherits_litestar_config_supports_overrides_and_bypass() -> None:
    auth_cfg = AiosqliteConfig(
        bind_key="auth",
        connection_config={"database": ":memory:"},
        extension_config={
            "litestar": {"session_key": "auth_session", "connection_key": "auth_conn", "pool_key": "auth_pool"}
        },
    )
    tenant_cfg = AsyncpgConfig(
        bind_key="primary_tenant",
        connection_config={"dsn": "postgresql://localhost/t1"},
        driver_features={"tenant_strategy": True},
        extension_config={
            "litestar": {
                "tenant_path_parameter": "org_id",
                "tenant_header": "x-org-id",
                "tenant_session_key": "active_org",
                "tenant_user_attr": "default_org",
                "tenant_memberships_attr": "org_ids",
                "tenant_variables_attr": "org_vars",
                "tenant_jwt_claim": "org_claim",
            }
        },
    )
    plugin = SQLSpecPlugin([auth_cfg, tenant_cfg])
    app = Litestar([], openapi_config=None, plugins=[plugin])

    path_scope: dict[str, Any] = {
        "type": "http",
        "user": {"org_ids": ("acme", "globex"), "org_vars": {"region": "us"}},
        "path_params": {"org_id": "globex"},
    }
    await _invoke_guard(require_tenant(), path_scope, app=app)
    assert await _resolve(path_scope) == TenantContextPayload("globex", variables={"region": "us"})

    claim_scope: dict[str, Any] = {"type": "http", "auth": SimpleNamespace(extras={"org_claim": "acme"})}
    await _invoke_guard(require_tenant(), claim_scope, app=app)
    assert await _resolve(claim_scope) == TenantContextPayload("acme")

    override_scope: dict[str, Any] = {
        "type": "http",
        "user": {"org_ids": ("acme", "globex")},
        "headers": [(b"x-custom-org", b"acme")],
    }
    await _invoke_guard(require_tenant(header="x-custom-org"), override_scope, app=app)
    assert await _resolve(override_scope) == TenantContextPayload("acme")

    bypass_scope: dict[str, Any] = {"type": "http"}
    await _invoke_guard(require_tenant(), bypass_scope, app=app, opt={"tenant_bypass": True})
    assert await _resolve(bypass_scope) == TenantContextPayload.bypass()


async def test_require_tenant_multi_tenant_config_disambiguation() -> None:
    first = AsyncpgConfig(
        bind_key="db_one",
        connection_config={"dsn": "postgresql://localhost/one"},
        driver_features={"tenant_strategy": True},
        extension_config={
            "litestar": {"session_key": "s1", "connection_key": "c1", "pool_key": "p1", "tenant_header": "x-one"}
        },
    )
    second = AsyncpgConfig(
        bind_key="db_two",
        connection_config={"dsn": "postgresql://localhost/two"},
        driver_features={"tenant_strategy": True},
        extension_config={
            "litestar": {"session_key": "s2", "connection_key": "c2", "pool_key": "p2", "tenant_header": "x-two"}
        },
    )
    app = Litestar([], openapi_config=None, plugins=[SQLSpecPlugin([first, second])])
    scope: dict[str, Any] = {
        "type": "http",
        "user": {"tenant_ids": ("acme", "globex")},
        "headers": [(b"x-two", b"globex")],
    }
    with pytest.raises(ImproperConfigurationError, match="config_key"):
        await _invoke_guard(require_tenant(), scope, app=app)

    await _invoke_guard(require_tenant(config_key="db_two"), scope, app=app)
    assert await _resolve(scope) == TenantContextPayload("globex")

    with pytest.raises(KeyError):
        await _invoke_guard(
            require_tenant(config_key="missing"), {"type": "http", "user": {"tenant_id": "acme"}}, app=app
        )


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        ({}, False),
        ({"user": Empty, "auth": Empty}, False),
        ({"user": None, "auth": None}, False),
        ({"user": SimpleNamespace(is_authenticated=False, user=None)}, False),
        (
            {
                "auth": SimpleNamespace(
                    evidence=(),
                    session=SimpleNamespace(is_available=False),
                    authorization=SimpleNamespace(
                        roles=frozenset(),
                        permissions=frozenset(),
                        scopes=frozenset(),
                        capabilities=frozenset(),
                        resources=frozenset(),
                        tenant_ids=frozenset(),
                        tenant_roles={"stale": frozenset()},
                        attributes={},
                    ),
                )
            },
            False,
        ),
        ({"user": SimpleNamespace(is_authenticated=True, user=None)}, True),
        (
            {
                "auth": SimpleNamespace(
                    evidence=(object(),),
                    session=SimpleNamespace(is_available=False),
                    authorization=SimpleNamespace(
                        roles=frozenset(), tenant_ids=frozenset(), tenant_roles={}, attributes={}
                    ),
                )
            },
            True,
        ),
        (
            {
                "auth": SimpleNamespace(
                    evidence=(),
                    session=SimpleNamespace(is_available=False),
                    authorization=SimpleNamespace(tenant_roles={"acme": frozenset({"reader"})}),
                )
            },
            True,
        ),
    ],
)
def test_is_scope_authenticated_handles_litestar_and_security_shapes(scope: dict[str, Any], expected: bool) -> None:
    assert is_scope_authenticated(scope) is expected


async def test_litestar_security_snapshot_restrictions_sessions_claims_and_predicates() -> None:
    empty_roles_scope: dict[str, Any] = {
        "user": SimpleNamespace(is_authenticated=True, user=SimpleNamespace(tenant_id="revoked")),
        "auth": SimpleNamespace(
            evidence=(object(),),
            session=SimpleNamespace(is_available=False),
            authorization=SimpleNamespace(
                tenant_ids=frozenset({"acme"}), tenant_roles={"revoked": frozenset(), "globex": frozenset({"editor"})}
            ),
        ),
    }
    assert await _resolve(empty_roles_scope) is None

    clamped_single_scope: dict[str, Any] = {
        "user": SimpleNamespace(is_authenticated=True, user=SimpleNamespace(tenant_id="acme")),
        "auth": SimpleNamespace(
            evidence=(object(),),
            session=SimpleNamespace(is_available=False),
            authorization=SimpleNamespace(tenant_ids=frozenset({"acme", "globex"})),
            restrictions=(
                SimpleNamespace(tenant_ids=frozenset({"globex", "initech"})),
                SimpleNamespace(tenant_ids=frozenset({"globex"})),
            ),
        ),
    }
    assert await _resolve(clamped_single_scope) == TenantContextPayload("globex")

    clamped_denied_path = {**clamped_single_scope, "path_params": {"tenant_id": "acme"}}
    assert await _resolve(clamped_denied_path) is None

    clamped_empty_scope: dict[str, Any] = {
        "user": SimpleNamespace(is_authenticated=True, user=SimpleNamespace(tenant_id="acme")),
        "auth": SimpleNamespace(
            evidence=(object(),),
            session=SimpleNamespace(is_available=False),
            authorization=SimpleNamespace(tenant_ids=frozenset({"acme"})),
            restrictions=(SimpleNamespace(tenant_ids=frozenset()),),
        ),
    }
    assert await _resolve(clamped_empty_scope) is None

    session_data = {"tenant_id": "globex"}
    available_handle = SimpleNamespace(is_available=True, get=lambda key, default=None: session_data.get(key, default))
    unavailable_handle = SimpleNamespace(
        is_available=False, get=lambda _key, _default=None: pytest.fail("unavailable session accessed")
    )
    handle_scope: dict[str, Any] = {
        "user": SimpleNamespace(is_authenticated=True, user=SimpleNamespace(tenant_ids=("acme", "globex"))),
        "session": unavailable_handle,
        "auth": SimpleNamespace(
            evidence=(object(),),
            session=available_handle,
            authorization=SimpleNamespace(tenant_ids=frozenset({"acme", "globex"})),
        ),
    }
    assert await _resolve(handle_scope) == TenantContextPayload("globex")

    claims_scope: dict[str, Any] = {
        "user": SimpleNamespace(
            is_authenticated=True,
            user=None,
            claims=SimpleNamespace(raw={"org": "acme", "tenant_variables": {"shard": "1"}}),
            extras={"tenant_ids": ("acme", "globex")},
        )
    }
    assert await _resolve(claims_scope, jwt_claim="org") == TenantContextPayload("acme", variables={"shard": "1"})

    nested_guard = SimpleNamespace(
        predicates=(
            SimpleNamespace(role="editor"),
            SimpleNamespace(predicate=SimpleNamespace(tenant_parameter="workspace_id")),
        )
    )
    predicate_scope: dict[str, Any] = {
        "user": SimpleNamespace(is_authenticated=True, user=None),
        "auth": SimpleNamespace(
            evidence=(object(),),
            session=unavailable_handle,
            authorization=SimpleNamespace(tenant_roles={"globex": frozenset({"editor"})}),
        ),
        "route_handler": SimpleNamespace(opt={}, resolve_guards=lambda: (nested_guard,)),
        "path_params": {"workspace_id": "globex"},
    }
    assert await _resolve(predicate_scope) == TenantContextPayload("globex")


def test_litestar_unguarded_routes_controller_bypass_and_websockets_translate_tenant_errors() -> None:
    seen_tenants: list[tuple[str | None, bool]] = []

    class _TenantSqliteConfig(SqliteConfig):
        @contextmanager
        def provide_connection(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
            payload = TenantContext.get()
            if payload is None:
                raise TenantContextError("Tenant context required")
            seen_tenants.append((TenantContext.get_tenant_id(), payload.is_bypass))
            saved = self.driver_features.pop("tenant_strategy", None)
            try:
                conn = self.create_connection()
            finally:
                if saved is not None:
                    self.driver_features["tenant_strategy"] = saved  # type: ignore[typeddict-unknown-key]
            try:
                yield conn
            finally:
                conn.close()

    config = _TenantSqliteConfig(
        connection_config={"database": ":memory:"},
        extension_config={"litestar": {"tenant_path_parameter": "org_id", "tenant_header": "x-org-id"}},
    )
    object.__setattr__(config, "driver_features", {**config.driver_features, "tenant_strategy": object()})

    class Auth(AbstractAuthenticationMiddleware):
        async def authenticate_request(self, connection: ASGIConnection[Any, Any, Any, Any]) -> AuthenticationResult:
            user_header = connection.headers.get("x-user")
            if not user_header:
                return AuthenticationResult(user=None, auth=None)
            if user_header == "multi":
                return AuthenticationResult(user={"tenant_ids": ("acme", "globex")}, auth={"authenticated": True})
            return AuthenticationResult(user={"tenant_id": user_header}, auth={"authenticated": True})

    class OrgController(Controller):
        path = "/orgs"
        guards = [require_tenant()]

        @get("/health", opt={"tenant_bypass": True})
        async def health(self, db_session: NamedDependency[Any]) -> dict[str, int]:
            return {"ok": int(db_session.select_value("SELECT 1"))}

        @get("/{org_id:str}/items")
        async def by_path(self, org_id: FromPath[str], db_session: NamedDependency[Any]) -> dict[str, Any]:
            return {"org": org_id, "value": int(db_session.select_value("SELECT 1"))}

    @get("/unguarded")
    async def unguarded(db_session: NamedDependency[Any]) -> dict[str, int]:
        return {"ok": int(db_session.select_value("SELECT 1"))}

    @websocket("/ws")
    async def ws_route(socket: WebSocket[Any, Any, Any], db_session: NamedDependency[Any]) -> None:
        await socket.accept()
        await socket.send_json({"ok": int(db_session.select_value("SELECT 1"))})
        await socket.close()

    plugin = SQLSpecPlugin(config)
    app = Litestar(
        [OrgController, unguarded, ws_route], openapi_config=None, plugins=[plugin], middleware=[DefineMiddleware(Auth)]
    )

    with TestClient(app=app, raise_server_exceptions=False) as client:
        assert client.get("/orgs/health").json() == {"ok": 1}
        assert seen_tenants[-1] == (None, True)

        assert client.get("/unguarded").status_code == 401
        assert client.get("/unguarded", headers={"x-user": "multi"}).status_code == 403
        assert client.get("/unguarded", headers={"x-user": "multi", "x-org-id": "evil"}).status_code == 403
        ok_header = client.get("/unguarded", headers={"x-user": "multi", "x-org-id": "globex"})
        assert ok_header.status_code == 200
        assert seen_tenants[-1] == ("globex", False)

        ok_path = client.get("/orgs/acme/items", headers={"x-user": "multi"})
        assert ok_path.status_code == 200
        assert ok_path.json() == {"org": "acme", "value": 1}
        assert seen_tenants[-1] == ("acme", False)
        assert client.get("/orgs/evil/items", headers={"x-user": "multi"}).status_code == 403

        with pytest.raises(WebSocketDisconnect) as ws_unauth, client.websocket_connect("/ws"):
            pass
        assert ws_unauth.value.code == 4001

        with (
            pytest.raises(WebSocketDisconnect) as ws_forbidden,
            client.websocket_connect("/ws", headers={"x-user": "multi", "x-org-id": "evil"}),
        ):
            pass
        assert ws_forbidden.value.code == 4003

        with client.websocket_connect("/ws", headers={"x-user": "acme"}) as ws:
            assert ws.receive_json() == {"ok": 1}
        assert seen_tenants[-1] == ("acme", False)

        with pytest.raises(NotAuthorizedException):
            plugin.provide_request_session_sync("db_connection", app.state, cast("Any", {"type": "http"}))
        with pytest.raises(PermissionDeniedException):
            plugin.provide_request_session_sync(
                "db_connection", app.state, cast("Any", {"type": "http", "user": {"tenant_ids": ("acme", "globex")}})
            )


async def test_provide_request_session_async_translates_missing_tenant() -> None:
    @asynccontextmanager
    async def never_called(_pool: Any) -> AsyncIterator[Any]:
        pytest.fail("provide_connection should not run without an authorized tenant")
        yield None

    cfg = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/db"}, driver_features={"tenant_strategy": True}
    )
    object.__setattr__(cfg, "provide_connection", never_called)
    plugin = SQLSpecPlugin(cfg)
    app = Litestar([], openapi_config=None, plugins=[plugin])
    app.state.db_pool = object()

    with pytest.raises(NotAuthorizedException):
        await plugin.provide_request_session_async("db_connection", app.state, cast("Any", {"type": "http"}))
    with pytest.raises(PermissionDeniedException):
        await plugin.provide_request_session_async(
            "db_connection", app.state, cast("Any", {"type": "http", "user": {"tenant_ids": ("acme", "globex")}})
        )
    with pytest.raises(WebSocketException) as ws_err:
        await plugin.provide_request_session_async("db_connection", app.state, cast("Any", {"type": "websocket"}))
    assert ws_err.value.code == 4001
