"""Session-context strategy behavior through recording native cursor seams."""

from typing import TYPE_CHECKING, Any, cast

import pytest

from sqlspec.adapters.pymssql.tenant import PymssqlSessionContextStrategy
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContextPayload

if TYPE_CHECKING:
    from sqlspec.adapters.pymssql import PymssqlConnection


class BatchConnection:
    def __init__(self, *, malformed: str = "", autocommit: bool = False) -> None:
        self.values: dict[str, str | None] = {"tenant_id": "prior"}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.closed = 0
        self.autocommit = autocommit
        self.malformed = malformed

    def cursor(self, **kwargs: Any) -> "BatchCursor":
        return BatchCursor(self)

    def commit(self) -> None:
        raise AssertionError("strategy committed")

    def rollback(self) -> None:
        raise AssertionError("strategy rolled back")


class BatchCursor:
    def __init__(self, conn: BatchConnection) -> None:
        self.conn = conn
        self.description: Any = None
        self.rows: list[tuple[Any, ...]] = []
        self.result = 0

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        self.conn.calls.append((sql, params))
        checkout = "@sqlspec_prior_raw_0" in sql
        count = len(params) // (4 if checkout else 3)
        keys = params[:count] if checkout else params[-count:]
        prior = tuple(self.conn.values.get(key) for key in keys)
        assignments = params[count : count * 3] if checkout else params[: count * 2]
        for key, value in zip(assignments[::2], assignments[1::2], strict=True):
            if self.conn.malformed != "mismatch":
                self.conn.values[key] = value
        self.rows = [(prior if checkout else ()) + tuple(self.conn.values.get(key) for key in keys)]
        aliases = ([f"sqlspec_prior_{i}" for i in range(count)] if checkout else []) + [
            f"sqlspec_current_{i}" for i in range(count)
        ]
        self.expected_description = tuple((name,) for name in aliases)
        if self.conn.malformed == "aliases":
            self.expected_description = (("wrong",),)
        if self.conn.malformed == "width":
            self.rows = [()]
        if self.conn.malformed == "no_row":
            self.rows = []
        if self.conn.malformed == "extra_row":
            self.rows *= 2

    def nextset(self) -> bool | None:
        self.result += 1
        if self.result == 1:
            return True
        if self.result == 2 and self.conn.malformed != "no_set":
            self.description = self.expected_description
            return True
        if self.result == 3:
            self.description = (("extra",),) if self.conn.malformed == "extra_set" else None
            return True
        return None

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows.pop(0) if self.rows else None

    def close(self) -> None:
        self.conn.closed += 1


@pytest.mark.parametrize("autocommit", [False, True])
def test_tenant_context_batches_capture_restore_and_binds(autocommit: bool) -> None:
    connection = BatchConnection(autocommit=autocommit)
    strategy = PymssqlSessionContextStrategy(variable_keys={"z": "zone", "a": "area"})
    payload = TenantContextPayload("a'; DROP TABLE x;--", variables={"z": "1", "a": "2"})
    state = strategy.checkout(cast("PymssqlConnection", connection), payload)
    assert state.baseline == {"tenant_id": "prior", "area": None, "zone": None}
    assert connection.values == {"tenant_id": payload.tenant_id, "area": "2", "zone": "1"}
    assert len(connection.calls) == 1
    sql, params = connection.calls[0]
    assert sql.count("%s") == 12
    assert payload.tenant_id not in sql
    assert params == (
        "tenant_id",
        "area",
        "zone",
        "tenant_id",
        payload.tenant_id,
        "area",
        "2",
        "zone",
        "1",
        "tenant_id",
        "area",
        "zone",
    )
    strategy.checkin(cast("PymssqlConnection", connection), state)
    assert len(connection.calls) == 2
    assert connection.calls[1][0].count("%s") == 9
    assert connection.values == {"tenant_id": "prior", "area": None, "zone": None}
    assert connection.closed == 2
    assert connection.autocommit is autocommit


@pytest.mark.parametrize("malformed", ["aliases", "width", "no_row", "extra_row", "extra_set", "no_set", "mismatch"])
@pytest.mark.parametrize("cleanup", [False, True])
def test_tenant_context_rejects_malformed_batch(malformed: str, cleanup: bool) -> None:
    conn = BatchConnection(malformed=malformed)
    strategy = PymssqlSessionContextStrategy()
    expected = TenantCleanupError if cleanup else TenantContextError
    with pytest.raises(expected) as raised:
        if cleanup:
            strategy.checkin(
                cast("PymssqlConnection", conn),
                TenantLeaseState(TenantContextPayload("a"), baseline={"tenant_id": "baseline"}),
            )
        else:
            strategy.checkout(cast("PymssqlConnection", conn), TenantContextPayload("a"))
    if malformed != "mismatch":
        assert isinstance(raised.value.__cause__, ValueError)
    assert len(conn.calls) == 1
    assert conn.closed == 1


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tenant_key": 1},
        {"tenant_key": "a;b"},
        {"tenant_key": "a.b"},
        {"tenant_key": "a" * 129},
        {"variable_keys": {"x": "tenant_id"}},
        {"variable_keys": {"x": 1}},
        {"variable_keys": {"x": "same", "y": "same"}},
    ],
)
def test_tenant_context_rejects_owned_key_collision(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ImproperConfigurationError):
        PymssqlSessionContextStrategy(**kwargs)


def test_tenant_context_unmapped_variable_and_empty_cleanup() -> None:
    conn = BatchConnection()
    strategy = PymssqlSessionContextStrategy()
    with pytest.raises(TenantContextError):
        strategy.checkout(cast("PymssqlConnection", conn), TenantContextPayload("a", variables={"region": "eu"}))
    strategy.checkin(cast("PymssqlConnection", conn), TenantLeaseState(TenantContextPayload("a"), baseline={}))
    assert conn.calls == []
    assert conn.closed == 0


def test_tenant_context_package_export() -> None:
    import sqlspec.adapters.pymssql as package

    assert package.PymssqlSessionContextStrategy is PymssqlSessionContextStrategy
    assert "PymssqlSessionContextStrategy" in package.__all__
