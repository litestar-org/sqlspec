"""External database boundary fixtures for Snowflake extension store tests."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest

from sqlspec.adapters.snowflake.config import SnowflakeConfig


def store_config(
    monkeypatch: pytest.MonkeyPatch, extension_config: dict[str, Any] | None = None
) -> tuple[SnowflakeConfig, MagicMock]:
    """Bind a Snowflake configuration to a recording database session boundary."""
    config = SnowflakeConfig(
        connection_config={"account": "test-account", "user": "test-user"}, extension_config=extension_config or {}
    )
    driver = MagicMock()
    driver.execute.return_value.get_affected_count.return_value = 1
    driver.execute.return_value.rows_affected = 1
    driver.select_one_or_none.return_value = None
    driver.select.return_value = []

    @contextmanager
    def provide_session(self: SnowflakeConfig, *args: Any, **kwargs: Any) -> Iterator[Any]:
        yield driver

    monkeypatch.setattr(SnowflakeConfig, "provide_session", provide_session)
    return config, driver
