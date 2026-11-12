"""Verified Db2 session global variables for row and column access control."""

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.core.tenant.base import TenantLeaseState, with_cause
from sqlspec.driver._tenant import AsyncTenantLease, SyncTenantLease
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError

if TYPE_CHECKING:
    from sqlspec.core.tenant.base import AsyncTenantStrategy, SyncTenantStrategy
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = (
    "DEFAULT_DB2_TENANT_VARIABLE",
    "Db2AsyncGlobalVariableStrategy",
    "Db2AsyncTenantLease",
    "Db2SyncGlobalVariableStrategy",
    "Db2SyncTenantLease",
    "validate_global_variable",
)

DEFAULT_DB2_TENANT_VARIABLE: Final = "APP.CURRENT_TENANT_ID"
_GLOBAL_VARIABLE: Final = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,127}\.[A-Za-z][A-Za-z0-9_]{0,127}")


def validate_global_variable(name: object) -> str:
    """Return a schema-qualified Db2 global variable name in its folded upper-case form.

    Raises:
        ImproperConfigurationError: The name is not a schema-qualified ordinary identifier.
    """
    if not isinstance(name, str) or _GLOBAL_VARIABLE.fullmatch(name) is None:
        msg = "Db2 tenant global variables must be schema-qualified identifiers such as APP.CURRENT_TENANT_ID"
        raise ImproperConfigurationError(msg)
    return name.upper()


def _read_sql(names: tuple[str, ...]) -> str:
    return f"SELECT {', '.join(names)} FROM SYSIBM.SYSDUMMY1"


def _assign_sql(names: tuple[str, ...]) -> str:
    if len(names) == 1:
        return f"SET {names[0]} = ?"
    return f"SET ({', '.join(names)}) = ({', '.join('?' for _ in names)})"


def _row_values(row: Any, count: int) -> tuple[str | None, ...]:
    if row is None or len(row) != count:
        msg = "Db2 tenant global variable read returned an unexpected row"
        raise ValueError(msg)
    if any(value is not None and not isinstance(value, str) for value in row):
        msg = "Db2 tenant global variables must hold character values"
        raise ValueError(msg)
    return tuple(cast("str | None", value) for value in row)


@mypyc_attr(allow_interpreted_subclasses=True)
class _Db2GlobalVariables:
    """Validate owned global variables and prepare their bound assignments."""

    __slots__ = ("_sorted_variable_keys", "_tenant_variable", "_variable_keys")

    def __init__(
        self, *, tenant_variable: object = DEFAULT_DB2_TENANT_VARIABLE, variable_keys: Mapping[str, str] | None = None
    ) -> None:
        """Configure the owned global variables.

        Args:
            tenant_variable: Schema-qualified global variable receiving the tenant identity.
            variable_keys: Payload variable names mapped to schema-qualified global variables.

        Raises:
            ImproperConfigurationError: A name is invalid or two keys target the same variable.
        """
        self._tenant_variable = validate_global_variable(tenant_variable)
        raw = cast("Mapping[object, object]", dict(variable_keys or {}))
        if any(not isinstance(key, str) or not key for key in raw):
            msg = "Tenant variable names must be non-empty strings"
            raise ImproperConfigurationError(msg)
        self._variable_keys = {cast("str", key): validate_global_variable(target) for key, target in raw.items()}
        targets = [self._tenant_variable, *self._variable_keys.values()]
        if len(set(targets)) != len(targets):
            msg = "Db2 tenant global variables must be distinct"
            raise ImproperConfigurationError(msg)
        self._sorted_variable_keys = tuple(sorted(self._variable_keys))

    def _assignments(self, payload: "TenantContextPayload") -> tuple[tuple[str, ...], tuple[str, ...]]:
        if payload.variables.keys() - self._variable_keys.keys():
            msg = "Tenant payload contains a variable without a Db2 global variable mapping"
            raise TenantContextError(msg)
        pairs = [(self._tenant_variable, payload.tenant_id)] + [
            (self._variable_keys[key], payload.variables[key])
            for key in self._sorted_variable_keys
            if key in payload.variables
        ]
        return tuple(name for name, _ in pairs), tuple(value for _, value in pairs)


@mypyc_attr(allow_interpreted_subclasses=True)
class Db2SyncGlobalVariableStrategy(_Db2GlobalVariables):
    """Set, verify, and restore Db2 session global variables on a synchronous connection.

    RCAC permissions and masks read the variables; database policies enforce row access.
    """

    __slots__ = ()

    def checkout(self, connection: Any, payload: "TenantContextPayload") -> TenantLeaseState:
        """Capture the previous values, assign the payload, and verify the server values."""
        names, values = self._assignments(payload)
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_read_sql(names))
                prior = _row_values(cursor.fetchone(), len(names))
                cursor.execute(_assign_sql(names), values)
                cursor.execute(_read_sql(names))
                current = _row_values(cursor.fetchone(), len(names))
            finally:
                cursor.close()
        except Exception as exc:
            msg = "Db2 tenant global variables could not be set"
            raise with_cause(TenantContextError(msg), exc)
        if current != values:
            msg = "Db2 tenant global variable verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload, dict(zip(names, prior, strict=True)))

    def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        """Restore the captured values and verify them before the connection is reused."""
        if not state.baseline:
            return
        names = tuple(state.baseline)
        values = tuple(state.baseline.values())
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_assign_sql(names), values)
                cursor.execute(_read_sql(names))
                restored = _row_values(cursor.fetchone(), len(names))
            finally:
                cursor.close()
        except Exception as exc:
            msg = "Db2 tenant global variables could not be restored"
            raise with_cause(TenantCleanupError(msg), exc)
        if restored != values:
            msg = "Db2 tenant global variable restore verification failed"
            raise TenantCleanupError(msg)


@mypyc_attr(allow_interpreted_subclasses=True)
class Db2AsyncGlobalVariableStrategy(_Db2GlobalVariables):
    """Set, verify, and restore Db2 session global variables on an asynchronous connection.

    RCAC permissions and masks read the variables; database policies enforce row access.
    """

    __slots__ = ()

    async def checkout(self, connection: Any, payload: "TenantContextPayload") -> TenantLeaseState:
        """Capture the previous values, assign the payload, and verify the server values."""
        names, values = self._assignments(payload)
        try:
            cursor = await connection.cursor()
            try:
                await cursor.execute(_read_sql(names))
                prior = _row_values(await cursor.fetchone(), len(names))
                await cursor.execute(_assign_sql(names), values)
                await cursor.execute(_read_sql(names))
                current = _row_values(await cursor.fetchone(), len(names))
            finally:
                await cursor.close()
        except Exception as exc:
            msg = "Db2 tenant global variables could not be set"
            raise with_cause(TenantContextError(msg), exc)
        if current != values:
            msg = "Db2 tenant global variable verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload, dict(zip(names, prior, strict=True)))

    async def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        """Restore the captured values and verify them before the connection is reused."""
        if not state.baseline:
            return
        names = tuple(state.baseline)
        values = tuple(state.baseline.values())
        try:
            cursor = await connection.cursor()
            try:
                await cursor.execute(_assign_sql(names), values)
                await cursor.execute(_read_sql(names))
                restored = _row_values(await cursor.fetchone(), len(names))
            finally:
                await cursor.close()
        except Exception as exc:
            msg = "Db2 tenant global variables could not be restored"
            raise with_cause(TenantCleanupError(msg), exc)
        if restored != values:
            msg = "Db2 tenant global variable restore verification failed"
            raise TenantCleanupError(msg)


@mypyc_attr(allow_interpreted_subclasses=True)
class Db2SyncTenantLease(SyncTenantLease):
    """Fail-closed tenant lease over one standalone Db2 connection per lease.

    The synchronous pool keeps one connection per thread, so each lease opens its own
    physical connection and closes it on release or invalidation.
    """

    __slots__ = ("_config",)

    def __init__(self, config: Any, strategy: "SyncTenantStrategy") -> None:
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


@mypyc_attr(allow_interpreted_subclasses=True)
class Db2AsyncTenantLease(AsyncTenantLease):
    """Fail-closed tenant lease over the asynchronous Db2 pool.

    Verified connections return to the pool; an unverified connection is discarded and closed.
    """

    __slots__ = ("_config", "_pool")

    def __init__(self, config: Any, strategy: "AsyncTenantStrategy") -> None:
        super().__init__(strategy)
        self._config = config
        self._pool: Any = None

    async def _acquire(self) -> Any:
        self._pool = await self._config.provide_pool()
        return await self._pool.acquire()

    async def _release(self, connection: Any) -> None:
        await self._pool.release(connection)

    async def _invalidate(self, connection: Any) -> None:
        await self._pool.discard(connection)

    async def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        await connection.rollback()
