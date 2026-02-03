# pyright: reportPrivateImportUsage = false, reportPrivateUsage = false
"""Unit tests for fast-path query cache behavior."""

from typing import Any

from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.core.compiler import OperationProfile
from sqlspec.core.parameters import ParameterInfo, ParameterProfile
from sqlspec.driver._common import CachedQuery, CommonDriverAttributesMixin, _QueryCache


class _FakeDriver(CommonDriverAttributesMixin):
    __slots__ = ()

    def _execute_raw(self, statement: Any, sql: str, params: Any) -> Any:
        return (statement, sql, params)


def test_query_cache_lru_eviction() -> None:
    cache = _QueryCache(max_size=2)

    cache.set("a", CachedQuery("SQL_A", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1))
    cache.set("b", CachedQuery("SQL_B", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1))
    assert cache.get("a") is not None

    cache.set("c", CachedQuery("SQL_C", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1))

    assert cache.get("b") is None
    assert cache.get("a") is not None
    assert cache.get("c") is not None


def test_query_cache_update_moves_to_end() -> None:
    cache = _QueryCache(max_size=2)

    cache.set("a", CachedQuery("SQL_A", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1))
    cache.set("b", CachedQuery("SQL_B", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1))
    cache.set("a", CachedQuery("SQL_A2", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 2))
    cache.set("c", CachedQuery("SQL_C", ParameterProfile.empty(), (), False, {}, "COMMAND", OperationProfile.empty(), 1))

    assert cache.get("b") is None
    entry = cache.get("a")
    assert entry is not None
    assert entry.compiled_sql == "SQL_A2"
    assert entry.param_count == 2


def test_try_fast_execute_cache_hit_rebinds() -> None:
    config = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK, supported_parameter_styles={ParameterStyle.QMARK}
        )
    )
    driver = _FakeDriver(object(), config)

    profile = ParameterProfile((ParameterInfo(None, ParameterStyle.QMARK, 0, 0, "?"),))
    cached = CachedQuery(
        compiled_sql="SELECT * FROM t WHERE id = ?",
        parameter_profile=profile,
        input_named_parameters=(),
        applied_wrap_types=False,
        parameter_casts={},
        operation_type="SELECT",
        operation_profile=OperationProfile(returns_rows=True, modifies_rows=False),
        param_count=1,
    )
    driver._query_cache.set("SELECT * FROM t WHERE id = ?", cached)

    result = driver._try_fast_execute("SELECT * FROM t WHERE id = ?", (1,))

    assert result is not None
    statement, sql, params = result
    assert sql == "SELECT * FROM t WHERE id = ?"
    assert params == (1,)
    assert statement.operation_type == "SELECT"
