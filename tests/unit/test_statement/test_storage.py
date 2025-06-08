from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.storage.protocol import StorageBackendProtocol
from sqlspec.storage.registry import BackendNotRegisteredError, StorageRegistry


@pytest.fixture
def registry() -> "StorageRegistry":
    return StorageRegistry()


def test_register_and_get_backend(registry: "StorageRegistry") -> None:
    backend = MagicMock(spec=StorageBackendProtocol)
    registry.register("foo", backend)
    assert registry.get("foo") is backend
    assert registry.is_registered("foo")
    assert "foo" in registry.list_keys()


def test_unregister_backend(registry: "StorageRegistry") -> None:
    backend = MagicMock(spec=StorageBackendProtocol)
    registry.register("bar", backend)
    registry.unregister("bar")
    assert not registry.is_registered("bar")
    with pytest.raises(BackendNotRegisteredError):
        registry.get("bar")


def test_duplicate_key_raises(registry: "StorageRegistry") -> None:
    backend = MagicMock(spec=StorageBackendProtocol)
    registry.register("dup", backend)
    with pytest.raises(ValueError):
        registry.register("dup", backend)


def test_invalid_key_raises(registry: "StorageRegistry") -> None:
    backend = MagicMock(spec=StorageBackendProtocol)
    with pytest.raises(TypeError):
        registry.register("", backend)
    with pytest.raises(TypeError):
        registry.register(123, backend)  # type: ignore


def test_invalid_backend_raises(registry: "StorageRegistry") -> None:
    with pytest.raises(TypeError):
        registry.register_backend("foo", None)  # type: ignore


def test_get_unregistered_backend_raises(registry: "StorageRegistry") -> None:
    with pytest.raises(BackendNotRegisteredError):
        registry.get("missing")


def test_register_from_config_obstore(registry: "StorageRegistry") -> None:
    config = {"backend_type": "obstore", "store_config": {"scheme": "s3", "bucket": "bkt"}, "base_path": "data"}
    with patch("sqlspec.storage.backends.obstore.ObstoreBackend.from_config") as mock_from_config:
        backend = MagicMock(spec=StorageBackendProtocol)
        mock_from_config.return_value = backend
        registry.register("ob", config)
        assert registry.get("ob") is backend
        mock_from_config.assert_called_once_with(config)


def test_register_from_config_fsspec(registry: "StorageRegistry") -> None:
    config = {"backend_type": "fsspec", "protocol": "s3", "fs_config": {"bucket": "bkt"}, "base_path": "data"}
    with patch("sqlspec.storage.backends.fsspec.FsspecBackend.from_config") as mock_from_config:
        backend = MagicMock(spec=StorageBackendProtocol)
        mock_from_config.return_value = backend
        registry.register("fs", config)
        assert registry.get("fs") is backend
        mock_from_config.assert_called_once_with(config)


def test_register_from_config_local(registry: "StorageRegistry") -> None:
    config = {"backend_type": "local", "base_path": "/tmp"}
    with patch("sqlspec.storage.backends.file.FSSpecBackend.from_config") as mock_from_config:
        backend = MagicMock(spec=StorageBackendProtocol)
        mock_from_config.return_value = backend
        registry.register("local", config)
        assert registry.get("local") is backend
        mock_from_config.assert_called_once_with(config)


def test_register_from_config_unknown_type(registry: "StorageRegistry") -> None:
    config = {"backend_type": "unknown"}
    with pytest.raises(ValueError):
        registry.register("bad", config)


def test_backend_type_and_base_uri_properties() -> None:
    # ObstoreBackend
    with patch("sqlspec.storage.backends.obstore.ObstoreBackend.__init__", return_value=None):
        from sqlspec.storage.backends.obstore import ObstoreBackend

        ob = ObstoreBackend.__new__(ObstoreBackend)
        ob._store_config = {"scheme": "s3", "bucket": "bkt"}
        assert ob.backend_type == "obstore"
        assert ob.base_uri == "s3://bkt"
    # FsspecBackend
    with patch("sqlspec.storage.backends.fsspec.FsspecBackend.__init__", return_value=None):
        from sqlspec.storage.backends.fsspec import FsspecBackend

        fs = FsspecBackend.__new__(FsspecBackend)
        fs._protocol = "s3"
        fs._fs_config = {"bucket": "bkt"}
        assert fs.backend_type == "fsspec"
        assert fs.base_uri == "s3://bkt"
    # FSSpecBackend
    with patch("sqlspec.storage.backends.file.FSSpecBackend.__init__", return_value=None):
        from sqlspec.storage.backends.fsspec import FSSpecBackend

        lf = FSSpecBackend.__new__(FSSpecBackend)
        lf._base_path = "/tmp"
        assert lf.backend_type == "local"
        assert lf.base_uri.startswith("file://")


def test_obstore_backend_methods(monkeypatch: "pytest.MonkeyPatch") -> None:
    from sqlspec.storage.backends.obstore import ObstoreBackend

    store_mock = MagicMock()
    monkeypatch.setattr("sqlspec.storage.backends.obstore.obstore.store.from_url", lambda **kwargs: store_mock)
    backend = ObstoreBackend(base_path="prefix", scheme="s3", bucket="bkt")
    # read_bytes
    backend.client.read_bytes.return_value = b"data"
    assert backend.read_bytes("file") == b"data"
    # write_bytes
    backend.write_bytes("file", b"data")
    backend.client.write_bytes.assert_called_with("file", b"data")
    # read_text
    backend.client.read_text.return_value = "txt"
    assert backend.read_text("file") == "txt"
    # write_text
    backend.write_text("file", "txt")
    backend.client.write_text.assert_called_with("file", "txt", encoding="utf-8")
    # exists
    backend.client.exists.return_value = True
    assert backend.exists("file") is True
    # delete
    backend.delete("file")
    backend.client.delete.assert_called_with("file")
    # list_files
    backend.client.list.return_value = [MagicMock(path="a"), MagicMock(path="b")]
    assert backend.list_files("pre") == ["a", "b"]
    # backend_type, base_uri
    assert backend.backend_type == "obstore"
    assert backend.base_uri == "s3://bkt"


def test_fsspec_backend_methods(monkeypatch: "pytest.MonkeyPatch") -> None:
    from sqlspec.storage.backends.fsspec import FsspecBackend

    # Mock the context manager returned by fsspec.open
    file_mock = MagicMock()
    file_mock.__enter__ = MagicMock(return_value=file_mock)
    file_mock.__exit__ = MagicMock(return_value=False)

    # Mock the filesystem for _get_fs, exists, delete, and list_files
    fs_mock = MagicMock()
    open_mock = MagicMock()
    open_mock.fs = fs_mock

    # Track the last call to fsspec.open to return our file mock
    def mock_open(uri: "Any", **kwargs: "Any") -> "Any":
        if "mode" in kwargs:
            return file_mock
        return open_mock

    monkeypatch.setattr("sqlspec.storage.backends.fsspec.fsspec.open", mock_open)

    backend = FsspecBackend(protocol="s3", base_path="prefix", bucket="bkt")

    # read_bytes
    file_mock.read.return_value = b"data"
    assert backend.read_bytes("file") == b"data"

    # write_bytes
    backend.write_bytes("file", b"data")
    file_mock.write.assert_called_with(b"data")

    # read_text
    file_mock.read.return_value = "txt"
    assert backend.read_text("file") == "txt"

    # write_text
    backend.write_text("file", "txt")
    file_mock.write.assert_called_with("txt")

    # exists
    fs_mock.exists.return_value = True
    assert backend.exists("file") is True

    # delete
    backend.delete("file")
    fs_mock.rm.assert_called_with("file")

    # list_files
    fs_mock.glob.return_value = ["a", "b"]
    fs_mock.isdir.return_value = False
    assert backend.list_files("pre") == ["a", "b"]

    # backend_type, base_uri
    assert backend.backend_type == "fsspec"
    assert backend.base_uri == "s3://bkt"


def test_localfile_backend_methods(tmp_path: "Path") -> None:
    from sqlspec.storage.backends.fsspec import FSSpecBackend

    backend = FSSpecBackend(str(tmp_path))
    # write_bytes/read_bytes
    backend.write_bytes("foo.bin", b"abc")
    assert backend.read_bytes("foo.bin") == b"abc"
    # write_text/read_text
    backend.write_text("foo.txt", "hello")
    assert backend.read_text("foo.txt") == "hello"
    # exists
    assert backend.exists("foo.txt") is True
    # list_files
    backend.write_text("bar.txt", "b")
    files = backend.list_files("")
    assert any("foo.txt" in f or "bar.txt" in f for f in files)
    # delete
    backend.delete("foo.txt")
    assert not backend.exists("foo.txt")
    # backend_type, base_uri
    assert backend.backend_type == "local"
    assert backend.base_uri.startswith("file://")
