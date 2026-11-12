"""Pure SQL Server tenant batch construction and response validation."""

import re
from collections.abc import Mapping
from functools import lru_cache
from typing import Any, Final, cast

from mypy_extensions import mypyc_attr

from sqlspec.core.tenant.base import TenantLeaseState
from sqlspec.exceptions import ImproperConfigurationError, TenantCleanupError, TenantContextError
from sqlspec.utils.tenant._context import TenantContextPayload

__all__ = (
    "DEFAULT_MSSQL_TENANT_KEY",
    "MssqlTenantSettings",
    "build_checkin_sql",
    "build_checkout_sql",
    "read_batch_result",
    "validate_session_key",
)


DEFAULT_MSSQL_TENANT_KEY: Final = "tenant_id"
_MAX_SESSION_KEY_LENGTH: Final = 128
_SESSION_KEY: Final = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _session_text(variable: str) -> str:
    return (
        f"CASE WHEN SQL_VARIANT_PROPERTY({variable}, 'BaseType') IN ('varchar', 'char') "
        f"THEN CAST(CAST({variable} AS VARCHAR(MAX)) AS NVARCHAR(MAX)) "
        f"ELSE CAST({variable} AS NVARCHAR(MAX)) END"
    )


def _assignments(count: int, placeholder: str) -> list[str]:
    return [
        f"DECLARE @sqlspec_key_{i} NVARCHAR(128) = {placeholder}; "
        f"DECLARE @sqlspec_value_{i} NVARCHAR(MAX) = {placeholder}; "
        f"DECLARE @sqlspec_variant_{i} SQL_VARIANT; "
        f"IF DATALENGTH(@sqlspec_value_{i}) > 8000 "
        f"SET @sqlspec_variant_{i} = CAST(@sqlspec_value_{i} AS VARCHAR(8000)); "
        f"ELSE SET @sqlspec_variant_{i} = CAST(@sqlspec_value_{i} AS NVARCHAR(4000)); "
        f"EXEC sys.sp_set_session_context @key = @sqlspec_key_{i}, @value = @sqlspec_variant_{i}, @read_only = 0;"
        for i in range(count)
    ]


def _current_values(count: int, placeholder: str) -> list[str]:
    return [
        f"DECLARE @sqlspec_current_raw_{i} SQL_VARIANT = SESSION_CONTEXT(CAST({placeholder} AS NVARCHAR(128)));"
        for i in range(count)
    ]


@lru_cache(maxsize=64)
def build_checkout_sql(count: int, placeholder: str) -> str:
    prior = [
        f"DECLARE @sqlspec_prior_raw_{i} SQL_VARIANT = SESSION_CONTEXT(CAST({placeholder} AS NVARCHAR(128))); "
        f"DECLARE @sqlspec_prior_{i} NVARCHAR(MAX) = " + _session_text(f"@sqlspec_prior_raw_{i}") + ";"
        for i in range(count)
    ]
    columns = [f"@sqlspec_prior_{i} AS sqlspec_prior_{i}" for i in range(count)] + [
        _session_text(f"@sqlspec_current_raw_{i}") + f" AS sqlspec_current_{i}" for i in range(count)
    ]
    return "\n".join([
        *prior,
        *_assignments(count, placeholder),
        *_current_values(count, placeholder),
        "SELECT " + ", ".join(columns) + ";",
    ])


@lru_cache(maxsize=64)
def build_checkin_sql(count: int, placeholder: str) -> str:
    columns = [_session_text(f"@sqlspec_current_raw_{i}") + f" AS sqlspec_current_{i}" for i in range(count)]
    return "\n".join([
        *_assignments(count, placeholder),
        *_current_values(count, placeholder),
        "SELECT " + ", ".join(columns) + ";",
    ])


def read_batch_result(cursor: Any, columns: tuple[str, ...]) -> tuple[str | None, ...]:
    while cursor.description is None:
        if not cursor.nextset():
            msg = "Missing tenant batch result"
            raise ValueError(msg)
    if tuple(column[0] for column in cursor.description) != columns:
        msg = "Malformed tenant batch metadata"
        raise ValueError(msg)
    row = cursor.fetchone()
    if row is None or len(row) != len(columns) or cursor.fetchone() is not None:
        msg = "Malformed tenant batch row"
        raise ValueError(msg)
    if any(value is not None and not isinstance(value, str) for value in row):
        msg = "Unexpected tenant session value type"
        raise ValueError(msg)
    while cursor.nextset():
        if cursor.description is not None:
            msg = "Unexpected tenant batch result"
            raise ValueError(msg)
    return tuple(row)


def validate_session_key(value: object) -> str:
    if not isinstance(value, str) or len(value) > _MAX_SESSION_KEY_LENGTH or _SESSION_KEY.fullmatch(value) is None:
        msg = "SQL Server tenant keys must match [A-Za-z_][A-Za-z0-9_]* and contain at most 128 characters"
        raise ImproperConfigurationError(msg)
    return value


@mypyc_attr(allow_interpreted_subclasses=True)
class MssqlTenantSettings:
    """Validate owned session keys and prepare verified session-context batches."""

    __slots__ = ("_sorted_variable_keys", "_tenant_key", "_variable_keys")

    def __init__(
        self, *, tenant_key: object = DEFAULT_MSSQL_TENANT_KEY, variable_keys: Mapping[str, str] | None = None
    ) -> None:
        self._tenant_key = validate_session_key(tenant_key)
        self._variable_keys = dict(variable_keys or {})
        keys = cast("Mapping[str, object]", self._variable_keys)
        targets = [self._tenant_key, *(validate_session_key(key) for key in keys.values())]
        if len(set(targets)) != len(targets):
            msg = "Tenant session keys must be distinct"
            raise ImproperConfigurationError(msg)
        self._sorted_variable_keys = tuple(sorted(self._variable_keys))

    def prepare_checkout(
        self, payload: TenantContextPayload, placeholder: str
    ) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
        """Return the checkout batch, its bound values, and the expected result columns.

        Raises:
            TenantContextError: The payload carries a variable without a session-key mapping.
        """
        if payload.variables.keys() - self._variable_keys.keys():
            msg = "Tenant variables require an explicit session-key mapping"
            raise TenantContextError(msg)
        pairs = [(self._tenant_key, payload.tenant_id)] + [
            (self._variable_keys[key], payload.variables[key])
            for key in self._sorted_variable_keys
            if key in payload.variables
        ]
        keys = tuple(key for key, _ in pairs)
        count = len(keys)
        params = keys + tuple(value for pair in pairs for value in pair) + keys
        columns = tuple(f"sqlspec_prior_{i}" for i in range(count)) + tuple(
            f"sqlspec_current_{i}" for i in range(count)
        )
        return build_checkout_sql(count, placeholder), params, columns

    def capture(
        self, payload: TenantContextPayload, params: tuple[str, ...], row: tuple[str | None, ...]
    ) -> TenantLeaseState:
        """Verify the applied values and capture the previous session values.

        Raises:
            TenantContextError: The server did not report the bound values.
        """
        count = len(params) // 4
        keys = params[:count]
        if row[count:] != params[count + 1 : 3 * count : 2]:
            msg = "Tenant session context verification failed"
            raise TenantContextError(msg)
        return TenantLeaseState(payload, baseline=dict(zip(keys, row[:count], strict=True)))

    def prepare_checkin(
        self, state: TenantLeaseState, placeholder: str
    ) -> tuple[str, tuple[str | None, ...], tuple[str, ...]]:
        """Return the restore batch, its bound values, and the expected result columns."""
        pairs = tuple(state.baseline.items())
        keys = tuple(key for key, _ in pairs)
        params = tuple(value for pair in pairs for value in pair) + keys
        columns = tuple(f"sqlspec_current_{i}" for i in range(len(keys)))
        return build_checkin_sql(len(keys), placeholder), params, columns

    def verify_checkin(self, state: TenantLeaseState, restored: tuple[str | None, ...]) -> None:
        """Require the server to report every captured value after restoration.

        Raises:
            TenantCleanupError: A restored value differs from its captured baseline.
        """
        if restored != tuple(state.baseline.values()):
            msg = "Tenant session context restore verification failed"
            raise TenantCleanupError(msg)
