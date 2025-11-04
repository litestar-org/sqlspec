"""Shared helpers for storage bridge integration tests."""

from typing import TYPE_CHECKING

from sqlspec.storage.registry import storage_registry

if TYPE_CHECKING:  # pragma: no cover
    from pytest_databases.docker.minio import MinioService

__all__ = ("register_minio_alias",)


def register_minio_alias(
    alias: str, minio_service: "MinioService", bucket: str, *, prefix: str = "storage-bridge"
) -> str:
    """Register a storage registry alias backed by the pytest-databases MinIO service."""

    storage_registry.register_alias(
        alias,
        f"s3://{bucket}/{prefix}",
        backend="fsspec",
        endpoint_url=f"http://{minio_service.endpoint}",
        key=minio_service.access_key,
        secret=minio_service.secret_key,
        use_ssl=False,
        client_kwargs={"endpoint_url": f"http://{minio_service.endpoint}", "verify": False},
    )
    return prefix
