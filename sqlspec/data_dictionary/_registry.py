import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = ("get_dialect_config", "list_registered_dialects", "normalize_dialect_name", "register_dialect")


_DIALECT_CONFIGS: dict[str, "DialectConfig"] = {}
_DIALECTS_LOADED = False

DIALECT_ALIASES: dict[str, str] = {"postgresql": "postgres", "mariadb": "mysql", "cockroach": "cockroachdb"}


def normalize_dialect_name(dialect: str) -> str:
    """Normalize dialect names to canonical registry keys.

    Args:
        dialect: Input dialect name.

    Returns:
        Canonical dialect key.
    """
    normalized = dialect.lower()
    return DIALECT_ALIASES.get(normalized, normalized)


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
    normalized = normalize_dialect_name(dialect)
    if normalized not in _DIALECT_CONFIGS:
        msg = f"Unknown dialect: {dialect}. Available: {', '.join(sorted(_DIALECT_CONFIGS.keys()))}"
        raise ValueError(msg)
    return _DIALECT_CONFIGS[normalized]


def list_registered_dialects() -> "list[str]":
    """Return registered dialect names.

    Returns:
        List of registered dialect names.
    """
    _load_default_dialects()
    return sorted(_DIALECT_CONFIGS.keys())
