"""RustFS-backed S3-compatible test helpers."""

import hashlib
import hmac
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import pytest

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
    """Create an S3 bucket idempotently via stdlib AWS SigV4 HTTP PUT without third-party SDKs."""
    parsed = urlsplit(endpoint_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        msg = f"S3 endpoint_url must be a valid http(s) URL, got {endpoint_url!r}"
        raise ValueError(msg)

    now = datetime.now(UTC)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_uri = f"/{bucket_name}"
    canonical_headers = f"host:{parsed.netloc}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = f"PUT\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    request_digest = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{request_digest}"
    signing_key = _derive_aws_v4_signing_key(secret_key, date_stamp=date_stamp, region=region, service="s3")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    bucket_url = f"{parsed.scheme}://{parsed.netloc}{canonical_uri}"
    request = Request(
        bucket_url,
        data=None,
        method="PUT",
        headers={
            "Authorization": authorization,
            "Content-Length": "0",
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        },
    )
    try:
        with urlopen(request, timeout=10) as response:
            _ = response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code == 409 and ("BucketAlreadyOwnedByYou" in body or "BucketAlreadyExists" in body):
            return bucket_name
        raise
    return bucket_name


def ensure_rustfs_bucket(rustfs_service: "RustfsService", bucket_name: str) -> str:
    """Ensure a bucket exists on a RustFS service without requiring s3fs or obstore."""
    return ensure_s3_bucket(
        rustfs_endpoint_url(rustfs_service),
        bucket_name,
        access_key=rustfs_service.access_key,
        secret_key=rustfs_service.secret_key,
    )


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


def rustfs_object_size(rustfs_service: "RustfsService", bucket_name: str, object_name: str) -> int:
    """Return the size of an object stored in RustFS."""
    info = rustfs_filesystem(rustfs_service).info(f"{bucket_name}/{object_name}")
    size = info.get("size")
    if isinstance(size, int):
        return size
    return 0


def _derive_aws_v4_signing_key(secret_key: str, *, date_stamp: str, region: str, service: str) -> bytes:
    """Derive an AWS Signature Version 4 signing key."""
    key_date = hmac.new(f"AWS4{secret_key}".encode(), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    key_region = hmac.new(key_date, region.encode("utf-8"), hashlib.sha256).digest()
    key_service = hmac.new(key_region, service.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(key_service, b"aws4_request", hashlib.sha256).digest()
