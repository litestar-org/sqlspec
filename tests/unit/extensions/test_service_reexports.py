"""Verify that framework extension packages re-export the SQLSpec service base classes.

The base classes live in :mod:`sqlspec.service`. Each first-party framework integration
re-exports them so consumers can pull the canonical service base from the framework
namespace they are already importing from.
"""

import importlib

import pytest

from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

pytestmark = pytest.mark.xdist_group("extensions")


@pytest.mark.parametrize(
    "module_name",
    [
        "sqlspec.extensions.litestar",
        "sqlspec.extensions.fastapi",
        "sqlspec.extensions.starlette",
        "sqlspec.extensions.sanic",
    ],
)
def test_async_and_sync_service_reexports(module_name: str) -> None:
    """Async + sync framework packages re-export both service base classes."""
    module = importlib.import_module(module_name)
    assert module.SQLSpecAsyncService is SQLSpecAsyncService
    assert module.SQLSpecSyncService is SQLSpecSyncService
    assert "SQLSpecAsyncService" in module.__all__
    assert "SQLSpecSyncService" in module.__all__


def test_flask_reexports_sync_only() -> None:
    """Flask is sync-only, so it re-exports the sync service base."""
    module = importlib.import_module("sqlspec.extensions.flask")
    assert module.SQLSpecSyncService is SQLSpecSyncService
    assert "SQLSpecSyncService" in module.__all__
