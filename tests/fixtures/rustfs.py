"""RustFS-backed S3-compatible test helpers."""

from typing import TYPE_CHECKING, Any

import pytest
from pytest_databases import create_s3_bucket

if TYPE_CHECKING:
    from pytest_databases.docker.rustfs import RustfsService

__all__ = (
    "ensure_rustfs_bucket",
    "ensure_s3_bucket",
    "rustfs_endpoint_url",
    "rustfs_filesystem",
    "rustfs_fsspec_kwargs",
    "rustfs_object_size",
    "rustfs_obstore_kwargs",
)


def ensure_s3_bucket(
    endpoint_url: str, bucket_name: str, *, access_key: str, secret_key: str, region: str = "us-east-1"
) -> str:
    """Create an S3 bucket idempotently via ``pytest_databases.create_s3_bucket``."""
    create_s3_bucket(endpoint_url, bucket_name, access_key=access_key, secret_key=secret_key, region=region)
    return bucket_name


def ensure_rustfs_bucket(rustfs_service: "RustfsService", bucket_name: str) -> str:
    """Ensure a bucket exists on a RustFS service without requiring s3fs or obstore."""
    rustfs_service.ensure_bucket(bucket_name)
    return bucket_name


def rustfs_endpoint_url(rustfs_service: "RustfsService") -> str:
    """Return the S3 endpoint URL for a pytest-databases RustFS service."""
    return rustfs_service.endpoint_url


def rustfs_fsspec_kwargs(rustfs_service: "RustfsService") -> dict[str, Any]:
    """Return FSSpec S3 options for a pytest-databases RustFS service."""
    endpoint_url = rustfs_service.endpoint_url
    return {
        "endpoint_url": endpoint_url,
        "key": rustfs_service.access_key,
        "secret": rustfs_service.secret_key,
        "use_ssl": rustfs_service.secure,
        "client_kwargs": {"endpoint_url": endpoint_url, "verify": False},
        "config_kwargs": {"s3": {"addressing_style": "path"}},
    }


def rustfs_obstore_kwargs(rustfs_service: "RustfsService") -> dict[str, Any]:
    """Return ObStore S3 options for a pytest-databases RustFS service."""
    return {
        "aws_endpoint": rustfs_service.endpoint_url,
        "aws_access_key_id": rustfs_service.access_key,
        "aws_secret_access_key": rustfs_service.secret_key,
        "aws_virtual_hosted_style_request": False,
        "client_options": {"allow_http": not rustfs_service.secure},
    }


def rustfs_filesystem(rustfs_service: "RustfsService") -> Any:
    """Create an S3 filesystem connected to the RustFS service."""
    pytest.importorskip("s3fs", reason="s3fs is required for RustFS S3 test setup")
    fsspec = pytest.importorskip("fsspec", reason="fsspec is required for RustFS S3 test setup")
    return fsspec.filesystem("s3", anon=False, **rustfs_fsspec_kwargs(rustfs_service))


def rustfs_object_size(rustfs_service: "RustfsService", bucket_name: str, object_name: str) -> int:
    """Return the size of an object stored in RustFS."""
    info = rustfs_filesystem(rustfs_service).info(f"{bucket_name}/{object_name}")
    size = info.get("size")
    if isinstance(size, int):
        return size
    return 0
