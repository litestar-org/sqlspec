"""Service transaction hooks commit on success and roll back without suppressing errors."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.aiosqlite import AiosqliteConfig, AiosqliteDriver
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver
from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("fail", [False, True])
async def test_async_begin_transaction_hooks(monkeypatch: pytest.MonkeyPatch, fail: bool) -> None:
    begin, commit, rollback = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(AiosqliteDriver, "begin", begin)
    monkeypatch.setattr(AiosqliteDriver, "commit", commit)
    monkeypatch.setattr(AiosqliteDriver, "rollback", rollback)
    config = AiosqliteConfig()
    try:
        async with config.provide_session() as session:
            service = SQLSpecAsyncService(session)
            if fail:
                with pytest.raises(ValueError, match="boom"):
                    async with service.begin_transaction() as bound:
                        assert bound is session
                        raise ValueError("boom")
                rollback.assert_awaited_once()
                commit.assert_not_awaited()
            else:
                async with service.begin_transaction() as bound:
                    assert bound is session
                commit.assert_awaited_once()
                rollback.assert_not_awaited()
            begin.assert_awaited_once()
            assert session._transaction_depth == 0
    finally:
        await config.close_pool()


@pytest.mark.parametrize("fail", [False, True])
def test_sync_begin_transaction_hooks(monkeypatch: pytest.MonkeyPatch, fail: bool) -> None:
    begin, commit, rollback = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr(SqliteDriver, "begin", begin)
    monkeypatch.setattr(SqliteDriver, "commit", commit)
    monkeypatch.setattr(SqliteDriver, "rollback", rollback)
    config = SqliteConfig()
    try:
        with config.provide_session() as session:
            service = SQLSpecSyncService(session)
            if fail:
                with pytest.raises(ValueError, match="boom"):
                    with service.begin_transaction() as bound:
                        assert bound is session
                        raise ValueError("boom")
                rollback.assert_called_once()
                commit.assert_not_called()
            else:
                with service.begin_transaction() as bound:
                    assert bound is session
                commit.assert_called_once()
                rollback.assert_not_called()
            begin.assert_called_once()
            assert session._transaction_depth == 0
    finally:
        config.close_pool()
