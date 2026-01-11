"""Configuration normalization helpers.

These utilities are used by adapter config modules to keep connection configuration handling
consistent across pooled and non-pooled adapters.
"""

from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import ImproperConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ("normalize_connection_config", "reject_pool_aliases")


def reject_pool_aliases(kwargs: "dict[str, Any]") -> None:
    """Reject legacy pool_config/pool_instance aliases.

    Args:
        kwargs: Keyword arguments passed to the adapter config constructor.

    Raises:
        ImproperConfigurationError: If deprecated pool aliases are supplied.
    """
    if "pool_config" in kwargs or "pool_instance" in kwargs:
        msg = (
            "pool_config and pool_instance are no longer supported. "
            "Use connection_config and connection_instance instead."
        )
        raise ImproperConfigurationError(msg)


def normalize_connection_config(
    connection_config: "Mapping[str, Any] | None", *, extra_key: str = "extra"
) -> "dict[str, Any]":
    """Normalize an adapter connection_config dictionary.

    This function:
    - Copies the provided mapping into a new dict.
    - Merges any nested dict stored under ``extra_key`` into the top-level config.
    - Ensures the extra mapping is a dictionary (or None).

    Args:
        connection_config: Raw connection configuration mapping.
        extra_key: Key holding additional keyword arguments to merge.

    Returns:
        Normalized connection configuration.

    Raises:
        ImproperConfigurationError: If ``extra_key`` exists but is not a dictionary.
    """
    normalized: dict[str, Any] = dict(connection_config) if connection_config else {}
    extras = normalized.pop(extra_key, {})
    if extras is None:
        return normalized
    if not isinstance(extras, dict):
        msg = f"The '{extra_key}' field in connection_config must be a dictionary."
        raise ImproperConfigurationError(msg)
    normalized.update(extras)
    return normalized
