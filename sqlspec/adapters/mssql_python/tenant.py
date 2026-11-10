"""Bound, verified SQL Server session-context tenant strategy."""

from collections.abc import Mapping
from typing import TYPE_CHECKING, Final

from sqlspec.adapters.mssql_python._typing import MssqlPythonConnection, MssqlPythonCursor
from sqlspec.core.tenant import TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant import TenantContextPayload
from sqlspec.utils.tenant_mssql import build_checkin_sql, build_checkout_sql, read_batch_result, validate_session_key

if TYPE_CHECKING:
    from sqlspec.adapters.mssql_python.config import MssqlPythonConfig
    from sqlspec.driver._tenant import SyncTenantLease

DEFAULT_MSSQL_TENANT_KEY: Final = "tenant_id"


class MssqlPythonSessionContextStrategy:
    """Apply and restore owned mutable keys with one verified SQL batch per boundary."""

    __slots__ = ("_sorted_variable_keys", "_tenant_key", "_variable_keys")

    def __init__(
        self, *, tenant_key: str = DEFAULT_MSSQL_TENANT_KEY, variable_keys: Mapping[str, str] | None = None
    ) -> None:
        self._tenant_key = validate_session_key(tenant_key)
        self._variable_keys = dict(variable_keys or {})
        targets = [self._tenant_key, *(validate_session_key(key) for key in self._variable_keys.values())]
        if len(set(targets)) != len(targets):
            msg = "Tenant session keys must be distinct"
            raise ImproperConfigurationError(msg)
        self._sorted_variable_keys = tuple(sorted(self._variable_keys))

    def checkout(self, connection: "MssqlPythonConnection", payload: TenantContextPayload) -> TenantLeaseState:
        """Capture previous settings, bind identity and verify the resulting values."""
        if payload.variables.keys() - self._variable_keys.keys():
            msg = "Tenant variables require an explicit session-key mapping"
            raise TenantContextError(msg)
        pairs = [(self._tenant_key, payload.tenant_id)] + [
            (self._variable_keys[key], payload.variables[key])
            for key in self._sorted_variable_keys
            if key in payload.variables
        ]
        keys = tuple(key for key, _ in pairs)
        params = keys + tuple(value for pair in pairs for value in pair) + keys
        count = len(keys)
        columns = tuple(f"sqlspec_prior_{i}" for i in range(count)) + tuple(
            f"sqlspec_current_{i}" for i in range(count)
        )
        try:
            with MssqlPythonCursor(connection) as cursor:
                cursor.execute(build_checkout_sql(count, "?"), params)
                row = read_batch_result(cursor, columns)
        except Exception as exc:
            msg = "Tenant session context checkout failed"
            raise TenantContextError(msg) from exc
        if row[count:] != tuple(value for _, value in pairs):
            msg = "Tenant session context verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload, baseline=dict(zip(keys, row[:count], strict=True)))

    def checkin(self, connection: "MssqlPythonConnection", state: TenantLeaseState) -> None:
        """Restore and verify only this lease's captured settings."""
        if not state.baseline:
            return
        pairs = tuple(state.baseline.items())
        keys = tuple(key for key, _ in pairs)
        params = tuple(value for pair in pairs for value in pair) + keys
        columns = tuple(f"sqlspec_current_{i}" for i in range(len(keys)))
        try:
            with MssqlPythonCursor(connection) as cursor:
                cursor.execute(build_checkin_sql(len(keys), "?"), params)
                restored = read_batch_result(cursor, columns)
        except Exception as exc:
            msg = "Tenant session context cleanup failed"
            raise TenantCleanupError(msg) from exc
        if restored != tuple(value for _, value in pairs):
            msg = "Tenant session context restore verification failed"
            raise TenantCleanupError(msg)


def build_mssql_python_tenant_lease(config: "MssqlPythonConfig") -> "SyncTenantLease":
    """Build an adapter-owned fail-closed tenant lease."""
    from sqlspec.driver._tenant import SyncTenantLease

    def acquire() -> "MssqlPythonConnection":
        return config.provide_pool().acquire()

    def release(connection: "MssqlPythonConnection") -> None:
        config.provide_pool().release(connection)

    def invalidate(connection: "MssqlPythonConnection") -> None:
        connection.close()

    def finish(connection: "MssqlPythonConnection", exc_type: type[BaseException] | None) -> None:
        if not connection.autocommit:
            connection.rollback()

    return SyncTenantLease(
        config.driver_features["tenant_strategy"],
        acquire=acquire,
        release=release,
        invalidate=invalidate,
        finish_transaction=finish,
    )
