"""Detection helpers for a wedged BigQuery emulator.

The goccy emulator executes query jobs synchronously inside the ``jobs.insert``
HTTP handler and can wedge its internal lock, after which it accepts requests
but never responds. With finite driver timeouts each call then fails fast, and
these helpers let fixtures skip the remaining BigQuery tests instead of paying
the timeout once per test.
"""

from concurrent.futures import TimeoutError as FuturesTimeoutError

import requests.exceptions
from google.api_core.exceptions import RetryError

__all__ = ("describe_wedge", "is_emulator_wedge")

_WEDGE_ERROR_TYPES = (
    TimeoutError,
    FuturesTimeoutError,
    RetryError,
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
)


def is_emulator_wedge(error: BaseException) -> bool:
    """Return True when an exception chain indicates the emulator stopped responding."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, _WEDGE_ERROR_TYPES):
            return True
        current = current.__cause__ or current.__context__
    return False


def describe_wedge(error: BaseException) -> str:
    """Summarize a wedge-classified exception for skip messages."""
    return f"{type(error).__name__}: {error}"
