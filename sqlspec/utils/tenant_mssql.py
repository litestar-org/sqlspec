"""Pure SQL Server tenant batch construction and response validation."""

import re
from typing import Any, Final

from sqlspec.exceptions import ImproperConfigurationError

__all__ = ("build_checkin_sql", "build_checkout_sql", "read_batch_result", "validate_session_key")


_MAX_SESSION_KEY_LENGTH: Final = 128


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
    if (
        not isinstance(value, str)
        or len(value) > _MAX_SESSION_KEY_LENGTH
        or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None
    ):
        msg = "SQL Server tenant keys must match [A-Za-z_][A-Za-z0-9_]* and contain at most 128 characters"
        raise ImproperConfigurationError(msg)
    return value
