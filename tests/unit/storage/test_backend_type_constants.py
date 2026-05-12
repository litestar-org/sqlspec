"""Tests for storage backend class constants."""

from typing import ClassVar, get_type_hints

import pytest

from sqlspec.storage.backends.fsspec import FSSpecBackend
from sqlspec.storage.backends.local import LocalStore
from sqlspec.storage.backends.obstore import ObStoreBackend

BackendClass = type[LocalStore] | type[FSSpecBackend] | type[ObStoreBackend]


def _slot_names(backend_cls: BackendClass) -> tuple[str, ...]:
    slots = backend_cls.__slots__
    if isinstance(slots, str):
        return (slots,)
    return tuple(slots)


@pytest.mark.parametrize(
    ("backend_cls", "backend_type", "expected_hint"),
    (
        (LocalStore, "local", ClassVar[str]),
        (FSSpecBackend, "fsspec", ClassVar[str]),
        (ObStoreBackend, "obstore", ClassVar[str]),
    ),
)
def test_backend_type_is_class_constant(backend_cls: BackendClass, backend_type: str, expected_hint: object) -> None:
    """Backend type is constant class metadata, not per-instance state."""
    assert backend_cls.backend_type == backend_type
    assert get_type_hints(backend_cls)["backend_type"] == expected_hint
    assert "backend_type" not in _slot_names(backend_cls)
