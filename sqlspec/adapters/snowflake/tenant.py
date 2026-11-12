"""Verified Snowflake immutable session attributes for row access policies."""

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.adapters.snowflake.core import connection_paramstyle
from sqlspec.core.tenant.base import TenantLeaseState, with_cause
from sqlspec.driver._tenant import SyncTenantLease
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError

if TYPE_CHECKING:
    from sqlspec.core.tenant.base import SyncTenantStrategy
    from sqlspec.utils.tenant import TenantContextPayload

__all__ = (
    "DEFAULT_SNOWFLAKE_TENANT_ATTRIBUTE",
    "SNOWFLAKE_SESSION_ATTRIBUTES_NAMESPACE",
    "SnowflakeSessionAttributeStrategy",
    "SnowflakeTenantLease",
    "validate_session_attribute",
)

DEFAULT_SNOWFLAKE_TENANT_ATTRIBUTE: Final = "tenant_id"
SNOWFLAKE_SESSION_ATTRIBUTES_NAMESPACE: Final = "SNOWFLAKE$SESSION_ATTRIBUTES"
_SESSION_ATTRIBUTE: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,254}")


def validate_session_attribute(name: object) -> str:
    """Return a Snowflake session attribute name unchanged.

    Session attribute names are case-sensitive, so the name is not folded.

    Raises:
        ImproperConfigurationError: The name is not an identifier of at most 255 characters.
    """
    if not isinstance(name, str) or _SESSION_ATTRIBUTE.fullmatch(name) is None:
        msg = "Snowflake tenant session attributes must match [A-Za-z_][A-Za-z0-9_]* and contain at most 255 characters"
        raise ImproperConfigurationError(msg)
    return name


def _placeholders(paramstyle: str, count: int) -> tuple[str, ...]:
    if paramstyle == "numeric":
        return tuple(f":{index}" for index in range(1, count + 1))
    if paramstyle in {"pyformat", "format"}:
        return tuple("%s" for _ in range(count))
    return tuple("?" for _ in range(count))


def _read_sql(paramstyle: str, count: int) -> str:
    namespace = f"'{SNOWFLAKE_SESSION_ATTRIBUTES_NAMESPACE}'"
    columns = [f"SYS_CONTEXT({namespace}, {placeholder})" for placeholder in _placeholders(paramstyle, count)]
    return "SELECT " + ", ".join(columns)


def _set_sql(paramstyle: str) -> str:
    namespace, key, value = _placeholders(paramstyle, 3)
    return f"CALL SET_SYS_CONTEXT({namespace}, {key}, {value})"


def _row_values(row: Any, count: int) -> tuple[str | None, ...]:
    values = tuple(row.values()) if isinstance(row, Mapping) else tuple(row) if row is not None else ()
    if len(values) != count:
        msg = "Snowflake session attribute read returned an unexpected row"
        raise ValueError(msg)
    if any(value is not None and not isinstance(value, str) for value in values):
        msg = "Snowflake session attributes must hold character values"
        raise ValueError(msg)
    return cast("tuple[str | None, ...]", values)


@mypyc_attr(allow_interpreted_subclasses=True)
class SnowflakeSessionAttributeStrategy:
    """Set and verify immutable Snowflake session attributes on a fresh connection.

    Row access policies read the values with ``SYS_CONTEXT('SNOWFLAKE$SESSION_ATTRIBUTES', '<name>')``.
    Snowflake cannot overwrite or clear an immutable attribute, so the session is never reused:
    the tenant lease opens one connection per lease and closes it on release.
    """

    __slots__ = ("_sorted_variable_keys", "_tenant_attribute", "_variable_keys")

    def __init__(
        self,
        *,
        tenant_attribute: object = DEFAULT_SNOWFLAKE_TENANT_ATTRIBUTE,
        variable_keys: Mapping[str, str] | None = None,
    ) -> None:
        """Configure the owned session attributes.

        Args:
            tenant_attribute: Session attribute receiving the tenant identity.
            variable_keys: Payload variable names mapped to session attribute names.

        Raises:
            ImproperConfigurationError: A name is invalid or two keys target the same attribute.
        """
        self._tenant_attribute = validate_session_attribute(tenant_attribute)
        raw = cast("Mapping[object, object]", dict(variable_keys or {}))
        if any(not isinstance(key, str) or not key for key in raw):
            msg = "Tenant variable names must be non-empty strings"
            raise ImproperConfigurationError(msg)
        self._variable_keys = {cast("str", key): validate_session_attribute(target) for key, target in raw.items()}
        targets = [self._tenant_attribute, *self._variable_keys.values()]
        if len(set(targets)) != len(targets):
            msg = "Snowflake tenant session attributes must be distinct"
            raise ImproperConfigurationError(msg)
        self._sorted_variable_keys = tuple(sorted(self._variable_keys))

    def _attributes(self, payload: "TenantContextPayload") -> tuple[tuple[str, ...], tuple[str, ...]]:
        if payload.variables.keys() - self._variable_keys.keys():
            msg = "Tenant payload contains a variable without a Snowflake session attribute mapping"
            raise TenantContextError(msg)
        pairs = [(self._tenant_attribute, payload.tenant_id)] + [
            (self._variable_keys[key], payload.variables[key])
            for key in self._sorted_variable_keys
            if key in payload.variables
        ]
        return tuple(name for name, _ in pairs), tuple(value for _, value in pairs)

    def checkout(self, connection: Any, payload: "TenantContextPayload") -> TenantLeaseState:
        """Require unset attributes, bind the payload, and verify the session values.

        Raises:
            TenantContextError: The session already carries an owned attribute, or binding or verification failed.
        """
        names, values = self._attributes(payload)
        paramstyle = connection_paramstyle(connection)
        read_sql = _read_sql(paramstyle, len(names))
        set_sql = _set_sql(paramstyle)
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(read_sql, names)
                prior = _row_values(cursor.fetchone(), len(names))
                if any(value is not None for value in prior):
                    msg = "Snowflake session already carries tenant session attributes"
                    raise TenantContextError(msg)
                for name, value in zip(names, values, strict=True):
                    cursor.execute(set_sql, (SNOWFLAKE_SESSION_ATTRIBUTES_NAMESPACE, name, value))
                cursor.execute(read_sql, names)
                current = _row_values(cursor.fetchone(), len(names))
            finally:
                cursor.close()
        except TenantContextError:
            raise
        except Exception as exc:
            msg = "Snowflake tenant session attributes could not be set"
            raise with_cause(TenantContextError(msg), exc)
        if current != values:
            msg = "Snowflake tenant session attribute verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload)

    def checkin(self, connection: Any, state: TenantLeaseState) -> None:
        """Leave the immutable attributes in place; the lease closes the session instead."""
        _ = connection, state


@mypyc_attr(allow_interpreted_subclasses=True)
class SnowflakeTenantLease(SyncTenantLease):
    """Fail-closed tenant lease over one standalone Snowflake connection per lease.

    Immutable session attributes cannot be cleared, so each lease opens its own physical
    connection and closes it on release or invalidation; it never returns to the pool.
    """

    __slots__ = ("_config",)

    def __init__(self, config: Any, strategy: "SyncTenantStrategy") -> None:
        super().__init__(strategy)
        self._config = config

    def _acquire(self) -> Any:
        return self._config.provide_pool().new_connection()

    def _release(self, connection: Any) -> None:
        try:
            connection.close()
            closed = connection.is_closed()
        except Exception as exc:
            msg = "Snowflake tenant connection could not be closed"
            raise with_cause(TenantCleanupError(msg), exc)
        if not closed:
            msg = "Snowflake tenant connection is still open after close"
            raise TenantCleanupError(msg)

    def _invalidate(self, connection: Any) -> None:
        connection.close()

    def _finish_transaction(self, connection: Any, exc_type: type[BaseException] | None) -> None:
        connection.rollback()
