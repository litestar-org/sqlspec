"""CockroachDB AsyncPG adapter helpers."""

import secrets
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ("CockroachAsyncpgRetryConfig", "calculate_backoff_seconds", "is_retryable_error")

# Retry configuration defaults (module-level for mypyc compatibility)
_DEFAULT_MAX_RETRIES: Final[int] = 10
_DEFAULT_BASE_DELAY_MS: Final[float] = 50.0
_DEFAULT_MAX_DELAY_MS: Final[float] = 5000.0
_DEFAULT_ENABLE_LOGGING: Final[bool] = True


@dataclass(frozen=True)
class CockroachAsyncpgRetryConfig:
    """CockroachDB asyncpg transaction retry configuration."""

    max_retries: int = _DEFAULT_MAX_RETRIES
    base_delay_ms: float = _DEFAULT_BASE_DELAY_MS
    max_delay_ms: float = _DEFAULT_MAX_DELAY_MS
    enable_logging: bool = _DEFAULT_ENABLE_LOGGING

    @classmethod
    def from_features(cls, driver_features: "Mapping[str, Any]") -> "CockroachAsyncpgRetryConfig":
        """Build retry config from driver feature mappings."""
        return cls(
            max_retries=int(driver_features.get("max_retries", _DEFAULT_MAX_RETRIES)),
            base_delay_ms=float(driver_features.get("retry_delay_base_ms", _DEFAULT_BASE_DELAY_MS)),
            max_delay_ms=float(driver_features.get("retry_delay_max_ms", _DEFAULT_MAX_DELAY_MS)),
            enable_logging=bool(driver_features.get("enable_retry_logging", _DEFAULT_ENABLE_LOGGING)),
        )


def is_retryable_error(error: BaseException) -> bool:
    """Return True when the error should trigger a CockroachDB retry."""
    if has_sqlstate(error):
        return str(error.sqlstate) == "40001"
    return False


def calculate_backoff_seconds(attempt: int, config: "CockroachAsyncpgRetryConfig") -> float:
    """Calculate exponential backoff delay in seconds."""
    base: float = config.base_delay_ms * (2**attempt)
    scale: int = 1000
    max_jitter: int = max(int(base * scale), 0)
    jitter: float = secrets.randbelow(max_jitter + 1) / scale if max_jitter else 0.0
    delay_ms: float = min(base + jitter, config.max_delay_ms)
    return delay_ms / 1000.0
