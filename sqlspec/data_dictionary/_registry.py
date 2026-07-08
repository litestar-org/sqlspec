import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlspec.data_dictionary._types import DialectConfig

__all__ = (
    "get_dialect_config",
    "list_registered_dialects",
    "normalize_dialect_mode",
    "normalize_dialect_name",
    "register_dialect",
)


_DIALECT_CONFIGS: dict[str, "DialectConfig"] = {}
_DIALECTS_LOADED: bool = False

DIALECT_ALIASES: dict[str, str] = {
    "postgresql": "postgres",
    "pg": "postgres",
    "cockroach": "cockroachdb",
    "tsql": "mssql",
    "sqlserver": "mssql",
}

DIALECT_MODE_ALIASES: dict[tuple[str, str], str] = {
    ("spanner", "google_sql"): "googlesql",
    ("spanner", "google-sql"): "googlesql",
    ("spanner", "google"): "googlesql",
    ("spanner", "postgres"): "postgresql",
    ("spanner", "pg"): "postgresql",
}


def normalize_dialect_name(dialect: str) -> str:
    """Normalize dialect names to canonical registry keys.

    Args:
        dialect: Input dialect name.

    Returns:
        Canonical dialect key.
    """
    normalized = dialect.lower()
    return DIALECT_ALIASES.get(normalized, normalized)


def normalize_dialect_mode(dialect: str, mode: str | None) -> str | None:
    """Normalize optional dialect mode names.

    Args:
        dialect: Canonical dialect name.
        mode: Optional dialect mode name.

    Returns:
        Canonical mode key, or None when no mode was provided.
    """
    if mode is None:
        return None
    normalized_dialect = normalize_dialect_name(dialect)
    normalized_mode = mode.lower()
    return DIALECT_MODE_ALIASES.get((normalized_dialect, normalized_mode), normalized_mode)


def _load_default_dialects() -> None:
    """Load built-in dialect configurations."""
    global _DIALECTS_LOADED
    if _DIALECTS_LOADED:
        return
    importlib.import_module("sqlspec.data_dictionary.dialects")
    _DIALECTS_LOADED = True


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
