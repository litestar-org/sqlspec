"""Typed parameter wrappers for explicit Oracle LOB / JSON binding.

These slot-based wrappers are an opt-in escape hatch for power users who need
deterministic LOB or JSON routing regardless of the size-based heuristics in
:func:`coerce_large_parameters_sync` / :func:`coerce_large_parameters_async`.

Wrap a parameter value to express explicit intent:

* :class:`OracleClob` — bind as ``DB_TYPE_CLOB`` regardless of length.
* :class:`OracleBlob` — bind as ``DB_TYPE_BLOB`` regardless of length.
* :class:`OracleJson` — express JSON intent; the input handler selects the
  storage type supported by the connected Oracle version.

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
    """Mark a value for version-aware Oracle JSON binding."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value


def is_json_payload(value: Any) -> bool:
    """Return True if the value should be claimed by the JSON input handler.

    OracleJson, dict, empty list/tuple, and list/tuple containing any non-numeric
    element are claimed. Non-empty sequences where all elements are int/float
    (excluding bool) are reserved for the vector handler unless wrapped in OracleJson.
    """
    if isinstance(value, OracleJson):
        return True
    if isinstance(value, dict):
        return True
    if isinstance(value, (list, tuple)):
        if not value:
            return True
        return not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in value)
    return False
