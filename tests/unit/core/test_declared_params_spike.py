"""SPIKE (throwaway): prove declared_parameters slot carriage + pool-leak safety.

De-risks the compiled/pooled SQL slot mechanics in isolation before Ch3 wires the
real ParameterDeclaration type. Validates all 7 propagation/reset sites. Folded into
Ch3 (sqlspec-smgc.3) and reverted afterward.
"""

from sqlspec.core._pool import get_sql_pool
from sqlspec.core.statement import SQL

_SENTINEL = ("declared-sentinel",)


def test_default_declared_parameters_is_empty_tuple() -> None:
    sql = SQL("select 1")
    assert sql.declared_parameters == ()


def test_copy_full_path_preserves_declared_parameters() -> None:
    sql = SQL("select :a")
    sql._declared_parameters = _SENTINEL
    new = sql.copy(statement="select :b")
    assert new.declared_parameters == _SENTINEL


def test_copy_fast_path_preserves_declared_parameters() -> None:
    # parameters-only fast path -> _create_empty_copy
    sql = SQL("select :a")
    sql._declared_parameters = _SENTINEL
    new = sql.copy(parameters={"a": 1})
    assert new.declared_parameters == _SENTINEL


def test_init_from_sql_object_preserves_declared_parameters() -> None:
    sql = SQL("select :a")
    sql._declared_parameters = _SENTINEL
    new = SQL(sql)
    assert new.declared_parameters == _SENTINEL


def test_reset_clears_declared_parameters() -> None:
    sql = SQL("select :a")
    sql._declared_parameters = _SENTINEL
    sql.reset()
    assert sql.declared_parameters == ()


def test_pool_recycle_does_not_leak_declared_parameters() -> None:
    """PRIMARY leak vector: a recycled SQL must NOT inherit a prior query's declarations."""
    pool = get_sql_pool()
    leaky = SQL("select :a")
    leaky._declared_parameters = _SENTINEL
    pool.release(leaky)  # resetter is SQL.reset -> must clear the slot

    recycled = pool.acquire()
    try:
        assert recycled._declared_parameters == ()
    finally:
        pool.release(recycled)
