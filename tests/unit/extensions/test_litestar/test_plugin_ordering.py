# pyright: reportArgumentType=false, reportAttributeAccessIssue=false, reportFunctionMemberAccess=false, reportReturnType=false
"""Regression tests for SQLSpecPlugin's interaction with downstream plugins.

Litestar walks ``app_config.plugins`` with a generator expression that captures
the list reference once. Plugins are allowed to register follow-on plugins by
mutating that list in place (``.append`` / ``.extend``). If any plugin rebinds
``app_config.plugins`` to a brand-new list, Litestar's iterator continues
walking the old list and any plugin appended afterwards is silently dropped.

These tests pin SQLSpecPlugin to the in-place mutation contract.
"""

import logging
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from litestar import Litestar
from litestar.config.app import AppConfig
from litestar.constants import HTTP_RESPONSE_START
from litestar.middleware import DefineMiddleware
from litestar.plugins import InitPluginProtocol

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.base import SQLSpec
from sqlspec.core import StatementConfig
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.extensions.litestar._utils import get_sqlspec_scope_state, set_sqlspec_scope_state
from sqlspec.extensions.litestar.handlers import autocommit_handler_maker, build_request_session
from sqlspec.extensions.litestar.plugin import (
    ConnectionCleanupMiddleware,
    CorrelationMiddleware,
    SQLCommenterMiddleware,
    SQLSpecPlugin,
    _ConnectionCleanupTarget,
)
from sqlspec.observability import ObservabilityConfig

if TYPE_CHECKING:
    from sqlspec.config import LifecycleConfig


def _build_sqlspec_plugin() -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(AiosqliteConfig(connection_config={"database": ":memory:"}))
    return SQLSpecPlugin(sqlspec=sqlspec)


class _FollowOnPlugin(InitPluginProtocol):
    """Marker plugin that records when its ``on_app_init`` fires."""

    def __init__(self, fired: list[str], name: str) -> None:
        self._fired = fired
        self._name = name

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        self._fired.append(self._name)
        return app_config


class _AppendsFollowOnPlugin(InitPluginProtocol):
    """Plugin that registers a follow-on plugin during its own ``on_app_init``.

    Mirrors the ``litestar-vite`` ``VitePlugin`` -> ``InertiaPlugin`` pattern
    that originally surfaced this bug.
    """

    def __init__(self, follow_on: InitPluginProtocol) -> None:
        self._follow_on = follow_on

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        app_config.plugins.append(self._follow_on)
        return app_config


def test_on_app_init_preserves_plugins_list_identity() -> None:
    """``app_config.plugins`` must be the same list object after ``on_app_init``.

    Rebinding to a new list breaks Litestar's mid-iteration plugin discovery —
    the iterator captured the old list reference and silently skips anything
    a later plugin appends to ``app_config.plugins``.
    """
    plugin = _build_sqlspec_plugin()
    app_config = AppConfig()
    original_plugins = app_config.plugins
    plugin.on_app_init(app_config)
    assert app_config.plugins is original_plugins, (
        "SQLSpecPlugin.on_app_init rebound app_config.plugins to a new list; this breaks downstream plugins that append to it during init."
    )


def test_follow_on_plugin_fires_when_sqlspec_registered_first() -> None:
    """[SQLSpec, Appender] order: appended follow-on plugin must still init."""
    fired: list[str] = []
    follow_on = _FollowOnPlugin(fired, "follow_on")
    appender = _AppendsFollowOnPlugin(follow_on)
    Litestar(plugins=[_build_sqlspec_plugin(), appender])
    assert "follow_on" in fired, (
        "Plugin appended to app_config.plugins during init was never called. SQLSpecPlugin must mutate app_config.plugins in place, not rebind it."
    )


def test_follow_on_plugin_fires_when_sqlspec_registered_second() -> None:
    """[Appender, SQLSpec] order: control case — must also fire."""
    fired: list[str] = []
    follow_on = _FollowOnPlugin(fired, "follow_on")
    appender = _AppendsFollowOnPlugin(follow_on)
    Litestar(plugins=[appender, _build_sqlspec_plugin()])
    assert "follow_on" in fired


class _ExistingMiddleware:
    def __init__(self, app: Any, **_: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        await self.app(scope, receive, send)


def _build_plugin(*, correlation: bool = False, sqlcommenter: bool = False, **litestar_settings: Any) -> SQLSpecPlugin:
    sqlspec = SQLSpec()
    sqlspec.add_config(
        AiosqliteConfig(
            connection_config={"database": ":memory:"},
            statement_config=StatementConfig(enable_sqlcommenter=sqlcommenter),
            extension_config={
                "litestar": {
                    "enable_correlation_middleware": correlation,
                    "enable_sqlcommenter_middleware": sqlcommenter,
                    **litestar_settings,
                }
            },
        )
    )
    return SQLSpecPlugin(sqlspec=sqlspec)


def _middleware_types(app_config: AppConfig) -> list[type[object]]:
    return [
        cast("type[object]", cast("DefineMiddleware", middleware).middleware)
        for middleware in app_config.middleware or []
    ]


def test_on_app_init_middleware_on_app_init_installs_both_middlewares_when_enabled() -> None:
    app_config = AppConfig()
    _build_plugin(correlation=True, sqlcommenter=True).on_app_init(app_config)
    assert _middleware_types(app_config) == [CorrelationMiddleware, SQLCommenterMiddleware, ConnectionCleanupMiddleware]


def test_on_app_init_middleware_on_app_init_installs_only_correlation_middleware() -> None:
    app_config = AppConfig()
    _build_plugin(correlation=True, sqlcommenter=False).on_app_init(app_config)
    assert _middleware_types(app_config) == [CorrelationMiddleware, ConnectionCleanupMiddleware]


def test_on_app_init_middleware_on_app_init_installs_only_sqlcommenter_middleware() -> None:
    app_config = AppConfig()
    _build_plugin(correlation=False, sqlcommenter=True).on_app_init(app_config)
    assert _middleware_types(app_config) == [SQLCommenterMiddleware, ConnectionCleanupMiddleware]


def test_on_app_init_middleware_on_app_init_installs_no_observability_middlewares_when_disabled() -> None:
    app_config = AppConfig()
    _build_plugin(correlation=False, sqlcommenter=False).on_app_init(app_config)
    assert _middleware_types(app_config) == [ConnectionCleanupMiddleware]


def test_on_app_init_middleware_on_app_init_preserves_existing_middlewares() -> None:
    existing = DefineMiddleware(_ExistingMiddleware)
    app_config = AppConfig(middleware=[existing])
    _build_plugin(correlation=True, sqlcommenter=True).on_app_init(app_config)
    assert _middleware_types(app_config)[:3] == [
        CorrelationMiddleware,
        SQLCommenterMiddleware,
        ConnectionCleanupMiddleware,
    ]
    assert app_config.middleware[-1] is existing


def test_existing_correlation_middleware_not_duplicated() -> None:
    existing = DefineMiddleware(CorrelationMiddleware, headers=("x-app-request-id",))
    app_config = AppConfig(middleware=[DefineMiddleware(_ExistingMiddleware), existing])
    _build_plugin(correlation=True, sqlcommenter=True).on_app_init(app_config)
    assert _middleware_types(app_config) == [
        SQLCommenterMiddleware,
        ConnectionCleanupMiddleware,
        _ExistingMiddleware,
        CorrelationMiddleware,
    ]
    assert app_config.middleware[-1] is existing
    assert existing.kwargs == {"headers": ("x-app-request-id",)}


def test_repeated_on_app_init_keeps_single_correlation_middleware() -> None:
    app_config = AppConfig()
    _build_plugin(correlation=True).on_app_init(app_config)
    _build_plugin(correlation=True).on_app_init(app_config)
    assert _middleware_types(app_config) == [CorrelationMiddleware, ConnectionCleanupMiddleware]


class _SubclassedCorrelationMiddleware(CorrelationMiddleware):
    __slots__ = ()


@pytest.mark.parametrize(
    "existing",
    [
        DefineMiddleware(_SubclassedCorrelationMiddleware, headers=("x-app-request-id",)),
        _SubclassedCorrelationMiddleware,
    ],
    ids=["define_middleware", "bare_class"],
)
def test_existing_correlation_middleware_subclass_not_duplicated(existing: Any) -> None:
    app_config = AppConfig(middleware=[existing])
    _build_plugin(correlation=True).on_app_init(app_config)
    assert len(app_config.middleware) == 2
    assert app_config.middleware[-1] is existing


@pytest.mark.parametrize(
    "litestar_settings",
    [{"correlation_header": "x-custom-id"}, {"correlation_headers": ["x-tenant-trace"]}, {"auto_trace_headers": False}],
    ids=["correlation_header", "correlation_headers", "auto_trace_headers"],
)
def test_skipped_correlation_middleware_warns_about_unapplied_header_settings(
    litestar_settings: "dict[str, Any]", caplog: pytest.LogCaptureFixture
) -> None:
    app_config = AppConfig(middleware=[DefineMiddleware(CorrelationMiddleware, headers=("x-app-request-id",))])
    plugin = _build_plugin(correlation=True, **litestar_settings)
    with caplog.at_level(logging.DEBUG, logger="sqlspec.extensions.litestar"):
        plugin.on_app_init(app_config)
    assert _correlation_skip_levels(caplog) == [logging.WARNING]


@pytest.mark.parametrize(
    ("existing", "litestar_settings"),
    [
        (DefineMiddleware(CorrelationMiddleware, headers=("x-app-request-id",)), {}),
        (
            DefineMiddleware(_SubclassedCorrelationMiddleware, headers=("x-custom-id", "x-tenant-trace")),
            {
                "correlation_header": "x-custom-id",
                "correlation_headers": ["x-tenant-trace"],
                "auto_trace_headers": False,
            },
        ),
    ],
    ids=["default_headers", "matching_headers"],
)
def test_skipped_correlation_middleware_does_not_warn(
    existing: DefineMiddleware, litestar_settings: "dict[str, Any]", caplog: pytest.LogCaptureFixture
) -> None:
    app_config = AppConfig(middleware=[existing])
    plugin = _build_plugin(correlation=True, **litestar_settings)
    with caplog.at_level(logging.DEBUG, logger="sqlspec.extensions.litestar"):
        plugin.on_app_init(app_config)
    assert _correlation_skip_levels(caplog) == [logging.DEBUG]
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]


def _correlation_skip_levels(caplog: pytest.LogCaptureFixture) -> "list[int]":
    return [
        record.levelno
        for record in caplog.records
        if getattr(record, "extra_fields", {}).get("stage") == "correlation_middleware_skipped"
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("scenario", "expected_op"),
    [("completed_stream", "commit"), ("raised_exception", "rollback"), ("incomplete_stream", "rollback")],
)
async def test_connection_cleanup_middleware_owns_deferred_streaming_sessions(scenario: str, expected_op: str) -> None:
    """ConnectionCleanupMiddleware commits completed deferred streams, rolls back failed or incomplete ones, emits session end, and detaches."""
    ended: list[Any] = []
    config = AsyncpgConfig(
        connection_config={"dsn": "postgresql://localhost/db"},
        observability_config=ObservabilityConfig(
            lifecycle=cast("LifecycleConfig", {"on_session_end": [lambda ctx: ended.append(ctx["session"])]})
        ),
    )
    conn = AsyncMock()
    conn.is_in_transaction = MagicMock(return_value=True)
    tx = AsyncMock()
    exited: list[tuple[Any, ...]] = []

    class _StreamCM:
        async def __aenter__(self) -> Any:
            return conn

        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
            exited.append((exc_type, exc_val, exc_tb))
            return False

    session = build_request_session(config, conn, "autocommit")

    before_send = autocommit_handler_maker("db_connection", session_scope_key="db_session_instance")
    target = _ConnectionCleanupTarget(
        connection_key="db_connection",
        session_key="db_session",
        commit_mode="autocommit",
        extra_commit_statuses=frozenset(),
        extra_rollback_statuses=frozenset(),
    )

    async def app(scope: Any, _receive: Any, send: Any) -> None:
        set_sqlspec_scope_state(scope, "db_connection", conn)
        set_sqlspec_scope_state(scope, "db_connection_ctx", _StreamCM())
        set_sqlspec_scope_state(scope, "db_session_instance", session)

        start_msg: Any = {"type": HTTP_RESPONSE_START, "status": 200, "headers": []}
        await send(start_msg)
        await before_send(start_msg, scope)
        session._transaction = tx

        chunk_msg: Any = {"type": "http.response.body", "body": b"chunk-1", "more_body": True}
        await send(chunk_msg)
        await before_send(chunk_msg, scope)
        assert get_sqlspec_scope_state(scope, "db_connection_stream_deferred") is True
        assert session.connection is conn

        if scenario == "raised_exception":
            raise RuntimeError("mid-stream crash")
        if scenario == "completed_stream":
            end_msg: Any = {"type": "http.response.body", "body": b"", "more_body": False}
            await send(end_msg)
            await before_send(end_msg, scope)
            assert session.connection is conn

    middleware = ConnectionCleanupMiddleware(app, targets=(target,))
    scope: Any = {"type": "http", "state": {}}
    if scenario == "raised_exception":
        with pytest.raises(RuntimeError, match="mid-stream crash"):
            await middleware(scope, AsyncMock(), AsyncMock())
    else:
        await middleware(scope, AsyncMock(), AsyncMock())

    if expected_op == "commit":
        tx.commit.assert_awaited_once()
        tx.rollback.assert_not_awaited()
    else:
        tx.rollback.assert_awaited_once()
        tx.commit.assert_not_awaited()
    assert ended == [session]
    assert len(exited) == 1
    assert get_sqlspec_scope_state(scope, "db_connection") is None
    assert get_sqlspec_scope_state(scope, "db_session_instance") is None
    assert get_sqlspec_scope_state(scope, "db_connection_stream_deferred") is None
    with pytest.raises(ImproperConfigurationError, match="already closed"):
        _ = session.connection.cursor
