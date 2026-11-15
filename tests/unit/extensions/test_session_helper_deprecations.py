"""Tests for the deprecated pre-db_session helper names."""

import importlib
from typing import Any

import pytest

from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

_SENTINEL = object()


@pytest.mark.parametrize(
    ("alias", "replacement"),
    [
        ("provide_request_session", "provide_db_session"),
        ("provide_request_session_sync", "provide_db_session_sync"),
        ("provide_request_connection", "provide_db_connection"),
        ("provide_request_connection_sync", "provide_db_connection_sync"),
    ],
)
def test_litestar_request_provider_aliases_warn_and_delegate(
    alias: str, replacement: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin = SQLSpecPlugin(SqliteConfig())
    received: list[Any] = []

    def replacement_method(self: Any, key: Any, state: Any, scope: Any) -> object:
        received.append((key, state, scope))
        return _SENTINEL

    monkeypatch.setattr(SQLSpecPlugin, replacement, replacement_method)

    with pytest.warns(DeprecationWarning, match=replacement):
        result = getattr(plugin, alias)("db_session", "state", "scope")

    assert result is _SENTINEL
    assert received == [("db_session", "state", "scope")]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("alias", "replacement"),
    [
        ("provide_request_session_async", "provide_db_session_async"),
        ("provide_request_connection_async", "provide_db_connection_async"),
    ],
)
async def test_litestar_async_request_provider_aliases_warn_and_delegate(
    alias: str, replacement: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    plugin = SQLSpecPlugin(SqliteConfig())

    async def replacement_method(self: Any, key: Any, state: Any, scope: Any) -> object:
        return _SENTINEL

    monkeypatch.setattr(SQLSpecPlugin, replacement, replacement_method)

    with pytest.warns(DeprecationWarning, match=replacement):
        result = await getattr(plugin, alias)("db_session", "state", "scope")

    assert result is _SENTINEL


@pytest.mark.parametrize(
    ("alias", "replacement", "arguments"),
    [
        ("session_provider_maker", "db_session_provider_maker", ("config", "db_connection")),
        ("connection_provider_maker", "db_connection_provider_maker", ("config", "db_pool", "db_connection")),
        ("pool_provider_maker", "db_pool_provider_maker", ("config", "db_pool")),
    ],
)
def test_litestar_provider_maker_aliases_warn_and_delegate(
    alias: str, replacement: str, arguments: tuple[str, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    handlers = importlib.import_module("sqlspec.extensions.litestar.handlers")
    monkeypatch.setattr(handlers, replacement, lambda *args, **kwargs: _SENTINEL)

    with pytest.warns(DeprecationWarning, match=replacement):
        result = getattr(handlers, alias)(*arguments)

    assert result is _SENTINEL


@pytest.mark.parametrize("framework", ["starlette", "sanic", "flask"])
def test_get_or_create_session_alias_warns_and_delegates(framework: str, monkeypatch: pytest.MonkeyPatch) -> None:
    utils = importlib.import_module(f"sqlspec.extensions.{framework}._utils")
    monkeypatch.setattr(utils, "get_or_create_db_session", lambda *args: (_SENTINEL, args))

    with pytest.warns(DeprecationWarning, match="get_or_create_db_session"):
        result = utils.get_or_create_session("first", "second")

    assert result == (_SENTINEL, ("first", "second"))
