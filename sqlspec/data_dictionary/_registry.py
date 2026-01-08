import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = ("get_dialect_config", "list_registered_dialects", "register_dialect")


_DIALECT_CONFIGS: dict[str, "DialectConfig"] = {}
_DIALECTS_LOADED = False


def _load_default_dialects() -> None:
    """Load built-in dialect configurations."""
    global _DIALECTS_LOADED
    if _DIALECTS_LOADED:
        return
    importlib.import_module("sqlspec.data_dictionary.dialects")
    _DIALECTS_LOADED = True  # pyright: ignore


def register_dialect(config: "DialectConfig") -> None:
    """Register a dialect configuration.

    Args:
        config: Dialect configuration to register.
    """
    _DIALECT_CONFIGS[config.name] = config


def get_dialect_config(dialect: str) -> "DialectConfig":
    """Get configuration for a dialect.

    Args:
        dialect: Dialect name.

    Returns:
        DialectConfig for the requested dialect.

    Raises:
        ValueError: When the dialect is unknown.
    """
    _load_default_dialects()
    if dialect not in _DIALECT_CONFIGS:
        msg = f"Unknown dialect: {dialect}. Available: {', '.join(sorted(_DIALECT_CONFIGS.keys()))}"
        raise ValueError(msg)
    return _DIALECT_CONFIGS[dialect]


def list_registered_dialects() -> "list[str]":
    """Return registered dialect names.

    Returns:
        List of registered dialect names.
    """
    _load_default_dialects()
    return sorted(_DIALECT_CONFIGS.keys())
