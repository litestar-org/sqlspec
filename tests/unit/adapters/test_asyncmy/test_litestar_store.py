# pyright: reportPrivateUsage=false
"""Unit tests for AsyncMy Litestar session store DDL configuration."""

from unittest.mock import MagicMock

from sqlspec.adapters.asyncmy.litestar import AsyncmyStore

DEFAULT_ENGINE_CLAUSE = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"


def _mock_config(litestar_config: "dict[str, object] | None" = None) -> MagicMock:
    config = MagicMock()
    config.extension_config = {"litestar": litestar_config or {}}
    return config


def test_asyncmy_litestar_store_default_ddl_unchanged() -> None:
    """Without table_options, the emitted DDL keeps today's exact ENGINE clause."""
    store = AsyncmyStore(_mock_config())

    ddl = store._table_ddl()

    assert ddl.strip().endswith(DEFAULT_ENGINE_CLAUSE)


def test_asyncmy_litestar_store_honors_table_options() -> None:
    """A configured table_options value is interpolated into the CREATE TABLE DDL."""
    store = AsyncmyStore(_mock_config({"table_options": "COMMENT='litestar-session'"}))

    ddl = store._table_ddl()

    assert f"{DEFAULT_ENGINE_CLAUSE} COMMENT='litestar-session'" in ddl
