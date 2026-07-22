"""Regression: the duckdb pool must surface on_connection_create failures, not silently swallow them."""

import pytest

from sqlspec.adapters.duckdb import DuckDBConfig

pytestmark = pytest.mark.xdist_group("duckdb")


def test_connection_create_failure_surfaces_real_error() -> None:
    """A raising on_connection_create propagates its real error instead of being suppressed."""

    def failing_hook(connection: object) -> None:
        raise RuntimeError("hook boom")

    config = DuckDBConfig(
        connection_config={"database": ":memory:"}, driver_features={"on_connection_create": failing_hook}
    )
    try:
        with pytest.raises(RuntimeError, match="hook boom"), config.provide_session() as session:
            session.execute("SELECT 1")
    finally:
        config.close_pool()
