"""RustFS-backed S3-compatible test helpers."""

from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from pytest_databases.docker.rustfs import RustfsService


def rustfs_endpoint_url(rustfs_service: "RustfsService") -> str:
    """Return the S3 endpoint URL for a pytest-databases RustFS service."""
    scheme = "https" if rustfs_service.secure else "http"
    return f"{scheme}://{rustfs_service.endpoint}"


def rustfs_fsspec_kwargs(rustfs_service: "RustfsService") -> dict[str, Any]:
    """Return FSSpec S3 options for a pytest-databases RustFS service."""
    endpoint_url = rustfs_endpoint_url(rustfs_service)
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
        "aws_endpoint": rustfs_endpoint_url(rustfs_service),
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


def ensure_rustfs_bucket(rustfs_service: "RustfsService", bucket_name: str) -> str:
    """Ensure the default RustFS bucket exists and return its name."""
    fs = rustfs_filesystem(rustfs_service)
    if not fs.exists(bucket_name):
        fs.mkdir(bucket_name)
    return bucket_name


def rustfs_object_size(rustfs_service: "RustfsService", bucket_name: str, object_name: str) -> int:
    """Return the size of an object stored in RustFS."""
    info = rustfs_filesystem(rustfs_service).info(f"{bucket_name}/{object_name}")
    size = info.get("size")
    if isinstance(size, int):
        return size
    return 0
