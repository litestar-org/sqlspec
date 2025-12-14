"""Configuration normalization helpers.

These utilities are used by adapter config modules to keep connection configuration handling
consistent across pooled and non-pooled adapters.
"""

from typing import TYPE_CHECKING, Any

from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.deprecation import warn_deprecation

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = ("apply_pool_deprecations", "normalize_connection_config")

_POOL_DEPRECATION_INFO = "Parameter renamed for consistency across pooled and non-pooled adapters"


def apply_pool_deprecations(
    *,
    kwargs: dict[str, Any],
    connection_config: "Any | None",
    connection_instance: "Any | None",
    version: str = "0.33.0",
    removal_in: str = "0.34.0",
) -> tuple["Any | None", "Any | None"]:
    """Apply deprecated pool_config/pool_instance arguments.

    Several adapters historically accepted ``pool_config`` and ``pool_instance``. SQLSpec standardized
    these to ``connection_config`` and ``connection_instance``. This helper preserves the prior
    behavior without repeating the same deprecation handling blocks in every adapter config.

    Args:
        kwargs: Keyword arguments passed to the adapter config constructor (mutated in-place).
        connection_config: Current connection_config value.
        connection_instance: Current connection_instance value.
        version: Version the parameters were deprecated in.
        removal_in: Version the parameters are scheduled for removal.

    Returns:
        Updated (connection_config, connection_instance).
    """
    if "pool_config" in kwargs:
        warn_deprecation(
            version=version,
            deprecated_name="pool_config",
            kind="parameter",
            removal_in=removal_in,
            alternative="connection_config",
            info=_POOL_DEPRECATION_INFO,
            stacklevel=3,
        )
        if connection_config is None:
            connection_config = kwargs.pop("pool_config")
        else:
            kwargs.pop("pool_config")

    if "pool_instance" in kwargs:
        warn_deprecation(
            version=version,
            deprecated_name="pool_instance",
            kind="parameter",
            removal_in=removal_in,
            alternative="connection_instance",
            info=_POOL_DEPRECATION_INFO,
            stacklevel=3,
        )
        if connection_instance is None:
            connection_instance = kwargs.pop("pool_instance")
        else:
            kwargs.pop("pool_instance")

    return connection_config, connection_instance


def normalize_connection_config(
    connection_config: "Mapping[str, Any] | None", *, extra_key: str = "extra"
) -> dict[str, Any]:
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
