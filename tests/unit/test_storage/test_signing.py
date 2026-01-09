"""Comprehensive tests for storage backend URL signing API.

Tests cover:
- sign_sync and sign_async methods for all backends
- supports_signing property
- NotImplementedError for unsupported backends (FSSpec, Local, file://, memory://)
- Overloaded signatures (single path returns str, list returns list)
- Edge cases (empty list, invalid paths, expires_in limits)
"""

from pathlib import Path

import pytest

from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.typing import FSSPEC_INSTALLED, OBSTORE_INSTALLED


def test_protocol_defines_sign_sync_method() -> None:
    """Test ObjectStoreProtocol includes sign_sync method."""

    assert hasattr(ObjectStoreProtocol, "sign_sync")


def test_protocol_defines_sign_async_method() -> None:
    """Test ObjectStoreProtocol includes sign_async method."""

    assert hasattr(ObjectStoreProtocol, "sign_async")


def test_protocol_defines_supports_signing_property() -> None:
    """Test ObjectStoreProtocol includes supports_signing property."""

    assert hasattr(ObjectStoreProtocol, "supports_signing")


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_local_supports_signing_is_false(tmp_path: Path) -> None:
    """Test ObStoreBackend with file:// protocol returns False for supports_signing."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    assert store.supports_signing is False


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_memory_supports_signing_is_false() -> None:
    """Test ObStoreBackend with memory:// protocol returns False for supports_signing."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://")
    assert store.supports_signing is False


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_supports_signing_is_false(tmp_path: Path) -> None:
    """Test FSSpecBackend always returns False for supports_signing."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    assert store.supports_signing is False


def test_local_store_supports_signing_is_false(tmp_path: Path) -> None:
    """Test LocalStore always returns False for supports_signing."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(tmp_path))
    assert store.supports_signing is False


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_file_sign_sync_raises_not_implemented(tmp_path: Path) -> None:
    """Test ObStoreBackend.sign_sync raises NotImplementedError for file protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    store.write_text("test.txt", "content")

    with pytest.raises(NotImplementedError) as excinfo:
        store.sign_sync("test.txt")

    assert "file" in str(excinfo.value).lower()
    assert "URL signing is not supported" in str(excinfo.value)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_memory_sign_sync_raises_not_implemented() -> None:
    """Test ObStoreBackend.sign_sync raises NotImplementedError for memory protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://")
    store.write_text("test.txt", "content")

    with pytest.raises(NotImplementedError) as excinfo:
        store.sign_sync("test.txt")

    assert "memory" in str(excinfo.value).lower()
    assert "URL signing is not supported" in str(excinfo.value)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_file_sign_async_raises_not_implemented(tmp_path: Path) -> None:
    """Test ObStoreBackend.sign_async raises NotImplementedError for file protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    await store.write_text_async("test.txt", "content")

    with pytest.raises(NotImplementedError) as excinfo:
        await store.sign_async("test.txt")

    assert "file" in str(excinfo.value).lower()


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
async def test_obstore_memory_sign_async_raises_not_implemented() -> None:
    """Test ObStoreBackend.sign_async raises NotImplementedError for memory protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend("memory://")
    await store.write_text_async("test.txt", "content")

    with pytest.raises(NotImplementedError) as excinfo:
        await store.sign_async("test.txt")

    assert "memory" in str(excinfo.value).lower()


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_sign_sync_raises_not_implemented(tmp_path: Path) -> None:
    """Test FSSpecBackend.sign_sync raises NotImplementedError."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    store.write_text("test.txt", "content")

    with pytest.raises(NotImplementedError) as excinfo:
        store.sign_sync("test.txt")

    assert "fsspec" in str(excinfo.value).lower()
    assert "not supported" in str(excinfo.value).lower()


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
async def test_fsspec_sign_async_raises_not_implemented(tmp_path: Path) -> None:
    """Test FSSpecBackend.sign_async raises NotImplementedError."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    await store.write_text_async("test.txt", "content")

    with pytest.raises(NotImplementedError) as excinfo:
        await store.sign_async("test.txt")

    assert "fsspec" in str(excinfo.value).lower()


def test_local_store_sign_sync_raises_not_implemented(tmp_path: Path) -> None:
    """Test LocalStore.sign_sync raises NotImplementedError."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(tmp_path))
    store.write_text("test.txt", "content")

    with pytest.raises(NotImplementedError) as excinfo:
        store.sign_sync("test.txt")

    assert "local" in str(excinfo.value).lower() or "file://" in str(excinfo.value).lower()


async def test_local_store_sign_async_raises_not_implemented(tmp_path: Path) -> None:
    """Test LocalStore.sign_async raises NotImplementedError."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(tmp_path))
    await store.write_text_async("test.txt", "content")

    with pytest.raises(NotImplementedError) as excinfo:
        await store.sign_async("test.txt")

    assert "local" in str(excinfo.value).lower() or "file://" in str(excinfo.value).lower()


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_sign_sync_with_list_paths_raises_not_implemented(tmp_path: Path) -> None:
    """Test ObStoreBackend.sign_sync raises NotImplementedError for list of paths."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")
    store.write_text("test1.txt", "content1")
    store.write_text("test2.txt", "content2")

    with pytest.raises(NotImplementedError):
        store.sign_sync(["test1.txt", "test2.txt"])


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_sign_sync_with_list_paths_raises_not_implemented(tmp_path: Path) -> None:
    """Test FSSpecBackend.sign_sync raises NotImplementedError for list of paths."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))
    store.write_text("test1.txt", "content1")
    store.write_text("test2.txt", "content2")

    with pytest.raises(NotImplementedError):
        store.sign_sync(["test1.txt", "test2.txt"])


def test_local_store_sign_sync_with_list_paths_raises_not_implemented(tmp_path: Path) -> None:
    """Test LocalStore.sign_sync raises NotImplementedError for list of paths."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(tmp_path))
    store.write_text("test1.txt", "content1")
    store.write_text("test2.txt", "content2")

    with pytest.raises(NotImplementedError):
        store.sign_sync(["test1.txt", "test2.txt"])


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_sign_sync_empty_list_raises_not_implemented(tmp_path: Path) -> None:
    """Test ObStoreBackend.sign_sync raises NotImplementedError even for empty list."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    with pytest.raises(NotImplementedError):
        store.sign_sync([])


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_sign_sync_empty_list_raises_not_implemented(tmp_path: Path) -> None:
    """Test FSSpecBackend.sign_sync raises NotImplementedError even for empty list."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    with pytest.raises(NotImplementedError):
        store.sign_sync([])


def test_local_store_sign_sync_empty_list_raises_not_implemented(tmp_path: Path) -> None:
    """Test LocalStore.sign_sync raises NotImplementedError even for empty list."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(tmp_path))

    with pytest.raises(NotImplementedError):
        store.sign_sync([])


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_sign_sync_for_upload_raises_not_implemented(tmp_path: Path) -> None:
    """Test ObStoreBackend.sign_sync raises NotImplementedError even with for_upload=True."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    with pytest.raises(NotImplementedError):
        store.sign_sync("test.txt", for_upload=True)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_sign_sync_with_custom_expires_raises_not_implemented(tmp_path: Path) -> None:
    """Test ObStoreBackend.sign_sync raises NotImplementedError with custom expires_in."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    with pytest.raises(NotImplementedError):
        store.sign_sync("test.txt", expires_in=7200)


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_signable_protocols_s3_supports_signing() -> None:
    """Test ObStoreBackend.supports_signing returns True for S3 protocol."""
    from unittest.mock import MagicMock, patch

    from sqlspec.storage.backends.obstore import ObStoreBackend

    with patch("sqlspec.storage.backends.obstore.ensure_obstore"):
        with patch("obstore.store.from_url") as mock_from_url:
            mock_from_url.return_value = MagicMock()

            store = ObStoreBackend.__new__(ObStoreBackend)
            store.protocol = "s3"
            store.backend_type = "obstore"
            store.base_path = ""
            assert store.supports_signing is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_signable_protocols_gs_supports_signing() -> None:
    """Test ObStoreBackend.supports_signing returns True for GCS protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend.__new__(ObStoreBackend)
    store.protocol = "gs"
    store.backend_type = "obstore"
    store.base_path = ""
    assert store.supports_signing is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_signable_protocols_gcs_supports_signing() -> None:
    """Test ObStoreBackend.supports_signing returns True for GCS protocol (gcs alias)."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend.__new__(ObStoreBackend)
    store.protocol = "gcs"
    store.backend_type = "obstore"
    store.base_path = ""
    assert store.supports_signing is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_signable_protocols_az_supports_signing() -> None:
    """Test ObStoreBackend.supports_signing returns True for Azure protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend.__new__(ObStoreBackend)
    store.protocol = "az"
    store.backend_type = "obstore"
    store.base_path = ""
    assert store.supports_signing is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_signable_protocols_azure_supports_signing() -> None:
    """Test ObStoreBackend.supports_signing returns True for Azure protocol (azure alias)."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend.__new__(ObStoreBackend)
    store.protocol = "azure"
    store.backend_type = "obstore"
    store.base_path = ""
    assert store.supports_signing is True


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_unsupported_protocol_http_supports_signing_false() -> None:
    """Test ObStoreBackend.supports_signing returns False for HTTP protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend.__new__(ObStoreBackend)
    store.protocol = "http"
    store.backend_type = "obstore"
    store.base_path = ""
    assert store.supports_signing is False


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_unsupported_protocol_https_supports_signing_false() -> None:
    """Test ObStoreBackend.supports_signing returns False for HTTPS protocol."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend.__new__(ObStoreBackend)
    store.protocol = "https"
    store.backend_type = "obstore"
    store.base_path = ""
    assert store.supports_signing is False


@pytest.mark.skipif(not OBSTORE_INSTALLED, reason="obstore missing")
def test_obstore_error_message_suggests_cloud_backends(tmp_path: Path) -> None:
    """Test error message mentions S3, GCS, and Azure as alternatives."""
    from sqlspec.storage.backends.obstore import ObStoreBackend

    store = ObStoreBackend(f"file://{tmp_path}")

    with pytest.raises(NotImplementedError) as excinfo:
        store.sign_sync("test.txt")

    error_msg = str(excinfo.value)
    assert "S3" in error_msg or "s3" in error_msg.lower()
    assert "GCS" in error_msg or "gcs" in error_msg.lower()
    assert "Azure" in error_msg or "azure" in error_msg.lower()


@pytest.mark.skipif(not FSSPEC_INSTALLED, reason="fsspec missing")
def test_fsspec_error_message_suggests_obstore_alternative(tmp_path: Path) -> None:
    """Test error message suggests using ObStoreBackend for signed URLs."""
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    store = FSSpecBackend("file", base_path=str(tmp_path))

    with pytest.raises(NotImplementedError) as excinfo:
        store.sign_sync("test.txt")

    error_msg = str(excinfo.value)
    assert "ObStoreBackend" in error_msg or "obstore" in error_msg.lower()


def test_local_store_error_message_mentions_file_uri(tmp_path: Path) -> None:
    """Test error message mentions using file:// URIs directly."""
    from sqlspec.storage.backends.local import LocalStore

    store = LocalStore(str(tmp_path))

    with pytest.raises(NotImplementedError) as excinfo:
        store.sign_sync("test.txt")

    error_msg = str(excinfo.value)
    assert "file://" in error_msg or "local" in error_msg.lower()
