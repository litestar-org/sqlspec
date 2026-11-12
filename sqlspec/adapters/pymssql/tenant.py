"""Bound, verified SQL Server session-context tenant strategy."""

from typing import TYPE_CHECKING, Any

from mypy_extensions import mypyc_attr

from sqlspec.adapters.pymssql._typing import PymssqlConnection, PymssqlCursor
from sqlspec.core.tenant.base import TenantLeaseState, with_cause
from sqlspec.driver._tenant import SyncTenantLease
from sqlspec.exceptions import TenantCleanupError, TenantContextError
from sqlspec.utils.tenant._mssql import MssqlTenantSettings, read_batch_result

if TYPE_CHECKING:
    from sqlspec.core.tenant.base import SyncTenantStrategy
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = ("PymssqlSessionContextStrategy", "PymssqlTenantLease")


@mypyc_attr(allow_interpreted_subclasses=True)
class PymssqlSessionContextStrategy(MssqlTenantSettings):
    """Apply and restore owned mutable keys with one verified SQL batch per boundary."""

    __slots__ = ()

    def checkout(self, connection: "PymssqlConnection", payload: "TenantContextPayload") -> TenantLeaseState:
        """Capture previous settings, bind identity and verify the resulting values."""
        sql, params, columns = self.prepare_checkout(payload, "%s")
        try:
            with PymssqlCursor(connection, as_dict=False) as cursor:
                cursor.execute(sql, params)
                row = read_batch_result(cursor, columns)
        except Exception as exc:
            msg = "Tenant session context checkout failed"
            raise with_cause(TenantContextError(msg), exc)
        return self.capture(payload, params, row)

    def checkin(self, connection: "PymssqlConnection", state: TenantLeaseState) -> None:
        """Restore and verify only this lease's captured settings."""
        if not state.baseline:
            return
        sql, params, columns = self.prepare_checkin(state, "%s")
        try:
            with PymssqlCursor(connection, as_dict=False) as cursor:
                cursor.execute(sql, params)
                restored = read_batch_result(cursor, columns)
        except Exception as exc:
            msg = "Tenant session context cleanup failed"
            raise with_cause(TenantCleanupError(msg), exc)
        self.verify_checkin(state, restored)


@mypyc_attr(allow_interpreted_subclasses=True)
class PymssqlTenantLease(SyncTenantLease):
    """Fail-closed tenant lease over pymssql connections."""

    __slots__ = ("_config",)

    def __init__(self, config: Any, strategy: "SyncTenantStrategy", *, session: bool = False) -> None:
        super().__init__(strategy)
        self._config = config

    def _acquire(self) -> Any:
        return self._config.provide_pool().new_connection()

    def _release(self, connection: Any) -> None:
        connection.close()

    def _invalidate(self, connection: Any) -> None:
        connection.close()

    def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        connection.rollback()
