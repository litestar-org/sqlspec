"""Spanner-family integration fixtures shared by driver and shared-contract tests."""

import logging
import sys
from collections.abc import Generator
from datetime import timedelta
from typing import TextIO, cast

import pytest
from google.cloud.spanner_v1._async.database_sessions_manager import (
    DatabaseSessionsManager as AsyncDatabaseSessionsManager,
)
from google.cloud.spanner_v1.database_sessions_manager import DatabaseSessionsManager


@pytest.fixture(scope="session", autouse=True)
def spanner_emulator_session_polling() -> Generator[None, None, None]:
    """Shorten the SDK's uninterruptible maintenance sleep in emulator tests.

    Remove this workaround when google-cloud-python#18317 is released.
    Multiplexed sessions and real database cleanup remain enabled.
    """
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(DatabaseSessionsManager, "_MAINTENANCE_THREAD_POLLING_INTERVAL", timedelta(milliseconds=100))
        patch.setattr(AsyncDatabaseSessionsManager, "_MAINTENANCE_THREAD_POLLING_INTERVAL", timedelta(milliseconds=100))
        yield


class _FilteredWriter:
    """Stream wrapper that drops noisy emulator lines."""

    __slots__ = ("_needle", "_stream")

    def __init__(self, stream: "TextIO", needle: str) -> None:
        self._stream = stream
        self._needle = needle

    def write(self, data: object) -> int:
        text = data.decode() if isinstance(data, bytes) else str(data)
        if self._needle in text:
            return len(text)
        return self._stream.write(text)

    def flush(self) -> None:
        self._stream.flush()

    def isatty(self) -> bool:
        return self._stream.isatty()

    def fileno(self) -> int:
        return self._stream.fileno()

    @property
    def encoding(self) -> str | None:
        return cast("str | None", getattr(self._stream, "encoding", None))


@pytest.fixture(scope="session", autouse=True)
def spanner_emulator_log_filter() -> Generator[None, None, None]:
    """Suppress noisy emulator session creation logs."""
    needle = "Created multiplexed session."
    stdout = sys.stdout
    stderr = sys.stderr
    sys.stdout = _FilteredWriter(stdout, needle)
    sys.stderr = _FilteredWriter(stderr, needle)
    logging.getLogger("database_sessions_manager").setLevel(logging.WARNING)
    try:
        yield
    finally:
        sys.stdout = stdout
        sys.stderr = stderr
