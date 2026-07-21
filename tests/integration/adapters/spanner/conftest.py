import logging
import sys
from collections.abc import Generator
from typing import TYPE_CHECKING, TextIO, cast

import pytest

from tests.integration.fixtures.spanner import drop_table_if_exists, run_ddl

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database


pytestmark = pytest.mark.xdist_group("spanner")


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


@pytest.fixture
def test_users_table(spanner_database: "Database") -> Generator[str, None, None]:
    """Create test_users table for CRUD tests."""
    table_name = "test_users"
    drop_table_if_exists(spanner_database, table_name)

    ddl = f"""
    CREATE TABLE {table_name} (
        id STRING(36) NOT NULL,
        name STRING(100),
        email STRING(255),
        age INT64
    ) PRIMARY KEY (id)
    """
    run_ddl(spanner_database, [ddl])

    yield table_name

    drop_table_if_exists(spanner_database, table_name)


@pytest.fixture
def test_arrow_table(spanner_database: "Database") -> Generator[str, None, None]:
    """Create test table for Arrow tests."""
    table_name = "test_arrow_data"
    drop_table_if_exists(spanner_database, table_name)

    ddl = f"""
    CREATE TABLE {table_name} (
        id INT64 NOT NULL,
        name STRING(100),
        value INT64
    ) PRIMARY KEY (id)
    """
    run_ddl(spanner_database, [ddl])

    yield table_name

    drop_table_if_exists(spanner_database, table_name)
