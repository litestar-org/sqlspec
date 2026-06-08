"""Transaction-context semantics for the SQLSpec service base classes.

These tests lock in the commit-on-success / rollback-on-error behavior of
``begin_transaction`` for both the sync and async service bases, and assert that
neither context manager suppresses exceptions raised inside the ``with`` body.
The non-suppression guarantee is what lets callers ``return`` from inside the
block without a trailing unreachable ``raise`` to satisfy type checkers.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.service import SQLSpecAsyncService, SQLSpecSyncService

pytestmark = pytest.mark.anyio


async def test_async_begin_transaction_commits_on_success() -> None:
    session = AsyncMock()
    service: SQLSpecAsyncService = SQLSpecAsyncService(session)

    async with service.begin_transaction() as bound:
        assert bound is session

    session.begin.assert_awaited_once()
    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()


async def test_async_begin_transaction_rolls_back_and_propagates() -> None:
    session = AsyncMock()
    service: SQLSpecAsyncService = SQLSpecAsyncService(session)

    with pytest.raises(ValueError, match="boom"):
        async with service.begin_transaction():
            raise ValueError("boom")

    session.begin.assert_awaited_once()
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()


def test_sync_begin_transaction_commits_on_success() -> None:
    session = MagicMock()
    service: SQLSpecSyncService = SQLSpecSyncService(session)

    with service.begin_transaction() as bound:
        assert bound is session

    session.begin.assert_called_once()
    session.commit.assert_called_once()
    session.rollback.assert_not_called()


def test_sync_begin_transaction_rolls_back_and_propagates() -> None:
    session = MagicMock()
    service: SQLSpecSyncService = SQLSpecSyncService(session)

    with pytest.raises(ValueError, match="boom"):
        with service.begin_transaction():
            raise ValueError("boom")

    session.begin.assert_called_once()
    session.rollback.assert_called_once()
    session.commit.assert_not_called()
