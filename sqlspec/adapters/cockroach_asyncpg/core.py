"""CockroachDB AsyncPG adapter helpers."""

import secrets
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ("CockroachRetryConfig", "calculate_backoff_seconds", "is_retryable_error")


@dataclass(frozen=True)
class CockroachRetryConfig:
    """CockroachDB transaction retry configuration."""

    max_retries: int = 10
    base_delay_ms: float = 50.0
    max_delay_ms: float = 5000.0
    enable_logging: bool = True

    @classmethod
    def from_features(cls, driver_features: "Mapping[str, Any]") -> "CockroachRetryConfig":
        """Build retry config from driver feature mappings."""
        return cls(
            max_retries=int(driver_features.get("max_retries", cls.max_retries)),
            base_delay_ms=float(driver_features.get("retry_delay_base_ms", cls.base_delay_ms)),
            max_delay_ms=float(driver_features.get("retry_delay_max_ms", cls.max_delay_ms)),
            enable_logging=bool(driver_features.get("enable_retry_logging", True)),
        )


def is_retryable_error(error: BaseException) -> bool:
    """Return True when the error should trigger a CockroachDB retry."""
    if has_sqlstate(error):
        return str(error.sqlstate) == "40001"
    return False


def calculate_backoff_seconds(attempt: int, config: "CockroachRetryConfig") -> float:
    """Calculate exponential backoff delay in seconds."""
    base: float = config.base_delay_ms * (2**attempt)
    scale: int = 1000
    max_jitter: int = max(int(base * scale), 0)
    jitter: float = secrets.randbelow(max_jitter + 1) / scale if max_jitter else 0.0
    delay_ms: float = min(base + jitter, config.max_delay_ms)
    return delay_ms / 1000.0
