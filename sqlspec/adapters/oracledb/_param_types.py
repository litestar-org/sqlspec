"""Typed parameter wrappers for explicit Oracle LOB / JSON binding.

These slot-based wrappers are an opt-in escape hatch for power users who need
deterministic LOB or JSON routing regardless of the size-based heuristics in
:func:`coerce_large_parameters_sync` / :func:`coerce_large_parameters_async`.

Wrap a parameter value to express explicit intent:

* :class:`OracleClob` — bind as ``DB_TYPE_CLOB`` regardless of length.
* :class:`OracleBlob` — bind as ``DB_TYPE_BLOB`` regardless of length.
* :class:`OracleJson` — bind as native JSON; defers to the C1 input handler.

The wrappers themselves perform no validation — type discipline lives at the
routing site so error messages can include database-context detail.
"""

from typing import Any

__all__ = ("OracleBlob", "OracleClob", "OracleJson")


class OracleClob:
    """Mark a value to be bound as ``DB_TYPE_CLOB`` regardless of length."""

    __slots__ = ("value",)

    def __init__(self, value: "str | bytes") -> None:
        self.value = value


class OracleBlob:
    """Mark a value to be bound as ``DB_TYPE_BLOB`` regardless of length."""

    __slots__ = ("value",)

    def __init__(self, value: "bytes | str") -> None:
        self.value = value


class OracleJson:
    """Mark a value to be bound as native ``DB_TYPE_JSON`` regardless of detected version."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value
