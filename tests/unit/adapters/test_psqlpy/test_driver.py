"""Unit tests for PsqlpyDriver wire-level sqlcommenter and _dml_count_query cache preservation."""

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest

from sqlspec.adapters.psqlpy import core as psqlpy_core
from sqlspec.adapters.psqlpy.core import build_statement_config
from sqlspec.adapters.psqlpy.driver import PsqlpyDriver
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
