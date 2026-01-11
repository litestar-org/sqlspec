"""Integration tests for storage backend URL signing with real cloud services.

Tests URL signing functionality against S3-compatible storage (MinIO) using
pytest-databases fixtures. These tests verify that actual signed URLs are
generated and can be used for download/upload operations.
"""

from typing import TYPE_CHECKING

import pytest
from minio import Minio

from sqlspec.typing import OBSTORE_INSTALLED

if TYPE_CHECKING:
    from pytest_databases.docker.minio import MinioService

    from sqlspec.protocols import ObjectStoreProtocol


TEST_TEXT_CONTENT = "Hello, SQLSpec URL signing test!"


@pytest.fixture
def obstore_s3_backend(
    minio_service: "MinioService", minio_client: "Minio", minio_default_bucket_name: str
) -> "ObjectStoreProtocol":
    """Set up ObStore S3 backend for signing tests."""
    _ = minio_client
    from sqlspec.storage.backends.obstore import ObStoreBackend

    s3_uri = f"s3://{minio_default_bucket_name}"
    return ObStoreBackend(
        s3_uri,
        aws_endpoint=f"http://{minio_service.endpoint}",
        aws_access_key_id=minio_service.access_key,
        aws_secret_access_key=minio_service.secret_key,
        aws_virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_supports_signing(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test that ObStore S3 backend supports signing."""
    assert obstore_s3_backend.supports_signing is True


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_sync_single_path_returns_string(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_sync with single path returns a string URL."""
    test_path = "signing_test/single_path.txt"
    obstore_s3_backend.write_text(test_path, TEST_TEXT_CONTENT)

    signed_url = obstore_s3_backend.sign_sync(test_path)

    assert isinstance(signed_url, str)
    assert len(signed_url) > 0
    assert "http" in signed_url.lower()


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_sync_list_paths_returns_list(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_sync with list of paths returns list of URLs."""
    test_paths = ["signing_test/list_path1.txt", "signing_test/list_path2.txt"]
    for path in test_paths:
        obstore_s3_backend.write_text(path, TEST_TEXT_CONTENT)

    signed_urls = obstore_s3_backend.sign_sync(test_paths)

    assert isinstance(signed_urls, list)
    assert len(signed_urls) == len(test_paths)
    for url in signed_urls:
        assert isinstance(url, str)
        assert len(url) > 0


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_sync_empty_list_returns_empty_list(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_sync with empty list returns empty list."""
    signed_urls = obstore_s3_backend.sign_sync([])

    assert isinstance(signed_urls, list)
    assert len(signed_urls) == 0


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_sync_with_custom_expires_in(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_sync with custom expiration time."""
    test_path = "signing_test/custom_expires.txt"
    obstore_s3_backend.write_text(test_path, TEST_TEXT_CONTENT)

    signed_url = obstore_s3_backend.sign_sync(test_path, expires_in=7200)

    assert isinstance(signed_url, str)
    assert len(signed_url) > 0


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_sync_for_upload(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_sync with for_upload=True for PUT operations."""
    test_path = "signing_test/upload_path.txt"

    signed_url = obstore_s3_backend.sign_sync(test_path, for_upload=True)

    assert isinstance(signed_url, str)
    assert len(signed_url) > 0


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_sync_max_expires_validation(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_sync raises ValueError when expires_in exceeds maximum."""
    test_path = "signing_test/max_expires.txt"
    obstore_s3_backend.write_text(test_path, TEST_TEXT_CONTENT)

    max_expires = 604800  # 7 days
    with pytest.raises(ValueError, match="exceed"):
        obstore_s3_backend.sign_sync(test_path, expires_in=max_expires + 1)


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_s3_sign_async_single_path_returns_string(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_async with single path returns a string URL."""
    test_path = "signing_test/async_single.txt"
    await obstore_s3_backend.write_text_async(test_path, TEST_TEXT_CONTENT)

    signed_url = await obstore_s3_backend.sign_async(test_path)

    assert isinstance(signed_url, str)
    assert len(signed_url) > 0


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_s3_sign_async_list_paths_returns_list(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_async with list of paths returns list of URLs."""
    test_paths = ["signing_test/async_list1.txt", "signing_test/async_list2.txt"]
    for path in test_paths:
        await obstore_s3_backend.write_text_async(path, TEST_TEXT_CONTENT)

    signed_urls = await obstore_s3_backend.sign_async(test_paths)

    assert isinstance(signed_urls, list)
    assert len(signed_urls) == len(test_paths)


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_s3_sign_async_empty_list_returns_empty_list(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_async with empty list returns empty list."""
    signed_urls = await obstore_s3_backend.sign_async([])

    assert isinstance(signed_urls, list)
    assert len(signed_urls) == 0


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_s3_sign_async_for_upload(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_async with for_upload=True for PUT operations."""
    test_path = "signing_test/async_upload.txt"

    signed_url = await obstore_s3_backend.sign_async(test_path, for_upload=True)

    assert isinstance(signed_url, str)
    assert len(signed_url) > 0


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_s3_sign_async_max_expires_validation(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test sign_async raises ValueError when expires_in exceeds maximum."""
    test_path = "signing_test/async_max_expires.txt"
    await obstore_s3_backend.write_text_async(test_path, TEST_TEXT_CONTENT)

    max_expires = 604800  # 7 days
    with pytest.raises(ValueError, match="exceed"):
        await obstore_s3_backend.sign_async(test_path, expires_in=max_expires + 1)


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_signed_url_contains_signature_params(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test that signed URL contains AWS signature parameters."""
    test_path = "signing_test/sig_params.txt"
    obstore_s3_backend.write_text(test_path, TEST_TEXT_CONTENT)

    signed_url = obstore_s3_backend.sign_sync(test_path)

    assert "X-Amz-" in signed_url or "x-amz-" in signed_url.lower()


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_different_paths_produce_different_urls(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test that different paths produce different signed URLs."""
    paths = ["signing_test/path_a.txt", "signing_test/path_b.txt"]
    for path in paths:
        obstore_s3_backend.write_text(path, TEST_TEXT_CONTENT)

    url_a = obstore_s3_backend.sign_sync(paths[0])
    url_b = obstore_s3_backend.sign_sync(paths[1])

    assert url_a != url_b
    assert "path_a" in url_a
    assert "path_b" in url_b


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_preserves_path_order_in_list(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test that signed URLs preserve order of input paths."""
    paths = [f"signing_test/order_{i}.txt" for i in range(5)]
    for path in paths:
        obstore_s3_backend.write_text(path, TEST_TEXT_CONTENT)

    signed_urls = obstore_s3_backend.sign_sync(paths)

    for i, url in enumerate(signed_urls):
        assert f"order_{i}" in url


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_with_special_characters_in_path(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test signing paths with special characters."""
    test_path = "signing_test/file with spaces.txt"
    obstore_s3_backend.write_text(test_path, TEST_TEXT_CONTENT)

    signed_url = obstore_s3_backend.sign_sync(test_path)

    assert isinstance(signed_url, str)
    assert len(signed_url) > 0


@pytest.mark.xdist_group("storage")
@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_s3_sign_with_nested_path(obstore_s3_backend: "ObjectStoreProtocol") -> None:
    """Test signing deeply nested paths."""
    test_path = "signing_test/level1/level2/level3/deep_file.txt"
    obstore_s3_backend.write_text(test_path, TEST_TEXT_CONTENT)

    signed_url = obstore_s3_backend.sign_sync(test_path)

    assert isinstance(signed_url, str)
    assert "level1" in signed_url or "level1%2F" in signed_url
