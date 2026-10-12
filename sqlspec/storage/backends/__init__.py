"""Storage backends."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlspec.storage.backends.base import ObjectStoreBase
    from sqlspec.storage.backends.fsspec import FSSpecBackend
    from sqlspec.storage.backends.local import LocalStore
    from sqlspec.storage.backends.obstore import ObStoreBackend

__all__ = ("FSSpecBackend", "LocalStore", "ObStoreBackend", "ObjectStoreBase")

_EXPORTS: dict[str, tuple[str, str]] = {
    "FSSpecBackend": ("sqlspec.storage.backends.fsspec", "FSSpecBackend"),
    "LocalStore": ("sqlspec.storage.backends.local", "LocalStore"),
    "ObStoreBackend": ("sqlspec.storage.backends.obstore", "ObStoreBackend"),
    "ObjectStoreBase": ("sqlspec.storage.backends.base", "ObjectStoreBase"),
}

_COMPILED = getattr(__spec__, "origin", "").endswith((".so", ".pyd"))


def __getattr__(name: str) -> Any:
    """Lazily resolve storage backend exports via PEP 562."""
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    value = getattr(import_module(module_path), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return sorted module attributes including lazy exports."""
    return sorted({*globals(), *__all__})


if _COMPILED:
    for _export_name in __all__:
        __getattr__(_export_name)
