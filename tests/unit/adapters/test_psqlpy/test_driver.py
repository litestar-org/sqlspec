"""Unit tests for PsqlpyDriver wire-level sqlcommenter and _dml_count_query cache preservation."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest

from sqlspec.adapters.psqlpy import core as psqlpy_core
from sqlspec.adapters.psqlpy.core import build_statement_config
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
from sqlspec.core import SQL
from sqlspec.core.sqlcommenter import SQLCommenterContext


@pytest.mark.anyio
async def test_psqlpy_dispatch_execute_preserves_dml_count_query_lru_cache_under_sqlcommenter() -> None:
    """Executing the same DML across 5 traceparent contexts causes only 1 _dml_count_query cache miss."""
    psqlpy_core._dml_count_query.cache_clear()
    fetched_queries: list[str] = []

    class _FakeResult:
        def result(self) -> list[dict[str, int]]:
            return [{psqlpy_core._DML_COUNT_COLUMN: 1}]

    def _fake_fetch(query: str, _params: Any) -> _FakeResult:
        fetched_queries.append(query)
        return _FakeResult()

    connection = SimpleNamespace(fetch=AsyncMock(side_effect=_fake_fetch), execute=AsyncMock())
    stmt_config = build_statement_config().replace(
        enable_sqlcommenter=True, sqlcommenter_enable_context=True, sqlcommenter_enable_traceparent=True
    )
    driver = PsqlpyDriver(connection=cast("Any", connection), statement_config=stmt_config)

    for idx in range(5):
        span_id = f"{idx + 1:016x}"
        with (
            patch(
                "sqlspec.core.sqlcommenter.get_trace_context",
                return_value=("0af7651916cd43dd8448eb211c80319c", span_id),
            ),
            SQLCommenterContext.scope({"route": f"/items/{idx}"}),
        ):
            stmt = driver.prepare_statement("UPDATE items SET qty = $1", (idx,))
            await driver.dispatch_execute(cast("Any", connection), stmt)

    cache_info = psqlpy_core._dml_count_query.cache_info()
    assert cache_info.misses == 1
    assert cache_info.hits == 4
    assert len(fetched_queries) == 5
    assert "0000000000000001" in fetched_queries[0]
    assert "0000000000000005" in fetched_queries[4]


@pytest.mark.anyio
async def test_psqlpy_dispatch_execute_script_pooler_mode_disables_prepared() -> None:
    """dispatch_execute_script in pooler mode passes prepared=False and emits script observer events."""
    from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime

    cursor = AsyncMock()
    cursor.execute = AsyncMock()
    driver = PsqlpyDriver(
        connection=cast("Any", cursor),
        statement_config=build_statement_config(),
        driver_features={"pooler_mode": "transaction"},
    )
    observed_events: list[Any] = []
    obs_config = ObservabilityConfig(statement_observers=(observed_events.append,))
    driver._observability = ObservabilityRuntime(obs_config)  # pyright: ignore[reportPrivateUsage]

    stmt = SQL("SELECT 1; SELECT 2", is_script=True, statement_config=build_statement_config())
    result = await driver.dispatch_execute_script(cast("Any", cursor), stmt)

    assert result.statement_count == 2
    assert result.successful_statements == 2
    assert cursor.execute.await_count == 2
    for call_args in cursor.execute.await_args_list:
        assert call_args.kwargs.get("prepared") is False

    assert len(observed_events) == 2
    assert observed_events[0].operation == "SCRIPT"
    assert observed_events[0].script_statement_count == 2


@pytest.mark.anyio
async def test_psqlpy_transaction_methods_raise_mapped_exceptions() -> None:
    """begin, commit, and rollback wrap driver errors in mapped exceptions."""
    from psqlpy.exceptions import OperationalError as PsqlpyOperationalError

    from sqlspec.exceptions import DatabaseConnectionError

    conn = AsyncMock()
    conn.in_transaction = False
    conn.execute = AsyncMock(side_effect=PsqlpyOperationalError("server closed the connection unexpectedly"))

    driver = PsqlpyDriver(connection=cast("Any", conn), statement_config=build_statement_config())

    with pytest.raises(DatabaseConnectionError):
        await driver.begin()

    driver._transaction_active = True  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(DatabaseConnectionError):
        await driver.commit()

    driver._transaction_active = True  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(DatabaseConnectionError):
        await driver.rollback()
