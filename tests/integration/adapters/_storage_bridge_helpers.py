"""Shared helpers for storage bridge integration tests."""

from typing import TYPE_CHECKING

from sqlspec.storage.registry import storage_registry
from tests.fixtures.rustfs import rustfs_fsspec_kwargs

if TYPE_CHECKING:  # pragma: no cover
    from pytest_databases.docker.rustfs import RustfsService

__all__ = ("register_rustfs_alias",)


def register_rustfs_alias(
    alias: str, rustfs_service: "RustfsService", bucket: str, *, prefix: str = "storage-bridge"
) -> str:
    """Register a storage registry alias backed by the pytest-databases RustFS service."""

    storage_registry.register_alias(
        alias, f"s3://{bucket}/{prefix}", backend="fsspec", **rustfs_fsspec_kwargs(rustfs_service)
    )
    return prefix
