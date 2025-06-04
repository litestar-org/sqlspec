from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sqlspec.storage.protocol import StorageBackendProtocol
from sqlspec.storage.registry import BackendNotRegisteredError, StorageRegistry


@pytest.fixture
def registry() -> "StorageRegistry":
    return StorageRegistry()


def test_register_and_get_backend(registry: "StorageRegistry") -> None:
    backend = MagicMock(spec=StorageBackendProtocol)
    registry.register_backend("foo", backend)
    assert registry.get_backend("foo") is backend
    assert registry.is_registered("foo")
    assert "foo" in registry.list_registered_keys()


def test_unregister_backend(registry: "StorageRegistry") -> None:
    backend = MagicMock(spec=StorageBackendProtocol)
    registry.register_backend("bar", backend)
    registry.unregister_backend("bar")
    assert not registry.is_registered("bar")
    with pytest.raises(BackendNotRegisteredError):
        registry.get_backend("bar")


def test_duplicate_key_raises(registry: "StorageRegistry") -> None:
    backend = MagicMock(spec=StorageBackendProtocol)
    registry.register_backend("dup", backend)
    with pytest.raises(ValueError):
        registry.register_backend("dup", backend)


def test_invalid_key_raises(registry: "StorageRegistry") -> None:
    backend = MagicMock(spec=StorageBackendProtocol)
    with pytest.raises(TypeError):
        registry.register_backend("", backend)
    with pytest.raises(TypeError):
        registry.register_backend(123, backend)  # type: ignore


def test_invalid_backend_raises(registry: "StorageRegistry") -> None:
    with pytest.raises(TypeError):
        registry.register_backend("foo", None)  # type: ignore


def test_get_unregistered_backend_raises(registry: "StorageRegistry") -> None:
    with pytest.raises(BackendNotRegisteredError):
        registry.get_backend("missing")


def test_register_from_config_obstore(registry: "StorageRegistry") -> None:
    config = {"backend_type": "obstore", "store_config": {"scheme": "s3", "bucket": "bkt"}, "base_path": "data"}
    with patch("sqlspec.storage.backends.obstore.ObstoreBackend.from_config") as mock_from_config:
        backend = MagicMock(spec=StorageBackendProtocol)
        mock_from_config.return_value = backend
        registry.register_from_config("ob", config)
        assert registry.get_backend("ob") is backend
        mock_from_config.assert_called_once_with(config)


def test_register_from_config_fsspec(registry: "StorageRegistry") -> None:
    config = {"backend_type": "fsspec", "protocol": "s3", "fs_config": {"bucket": "bkt"}, "base_path": "data"}
    with patch("sqlspec.storage.backends.fsspec.FsspecBackend.from_config") as mock_from_config:
        backend = MagicMock(spec=StorageBackendProtocol)
        mock_from_config.return_value = backend
        registry.register_from_config("fs", config)
        assert registry.get_backend("fs") is backend
        mock_from_config.assert_called_once_with(config)


def test_register_from_config_local(registry: "StorageRegistry") -> None:
    config = {"backend_type": "local", "base_path": "/tmp"}
    with patch("sqlspec.storage.backends.file.LocalFileBackend.from_config") as mock_from_config:
        backend = MagicMock(spec=StorageBackendProtocol)
        mock_from_config.return_value = backend
        registry.register_from_config("local", config)
        assert registry.get_backend("local") is backend
        mock_from_config.assert_called_once_with(config)


def test_register_from_config_unknown_type(registry: "StorageRegistry") -> None:
    config = {"backend_type": "unknown"}
    with pytest.raises(ValueError):
        registry.register_from_config("bad", config)


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
    # LocalFileBackend
    with patch("sqlspec.storage.backends.file.LocalFileBackend.__init__", return_value=None):
        from sqlspec.storage.backends.file import LocalFileBackend

        lf = LocalFileBackend.__new__(LocalFileBackend)
        lf._base_path = "/tmp"
        assert lf.backend_type == "local"
        assert lf.base_uri.startswith("file://")


def test_obstore_backend_methods(monkeypatch: "pytest.MonkeyPatch") -> None:
    from sqlspec.storage.backends.obstore import ObstoreBackend

    store_mock = MagicMock()
    monkeypatch.setattr("sqlspec.storage.backends.obstore.obstore.ObjectStore", lambda **kwargs: store_mock)
    backend = ObstoreBackend({"scheme": "s3", "bucket": "bkt"}, base_path="prefix")
    # read_bytes
    store_mock.read_bytes.return_value = b"data"
    assert backend.read_bytes("file") == b"data"
    # write_bytes
    backend.write_bytes("file", b"data")
    store_mock.write_bytes.assert_called_with("prefix/file", b"data")
    # read_text
    store_mock.read_text.return_value = "txt"
    assert backend.read_text("file") == "txt"
    # write_text
    backend.write_text("file", "txt")
    store_mock.write_text.assert_called_with("prefix/file", "txt", encoding="utf-8")
    # exists
    store_mock.exists.return_value = True
    assert backend.exists("file") is True
    # delete
    backend.delete("file")
    store_mock.delete.assert_called_with("prefix/file")
    # list_objects
    store_mock.list_objects.return_value = ["a", "b"]
    assert backend.list_objects("pre") == ["a", "b"]  # type: ignore[attr-defined]
    # backend_type, base_uri
    assert backend.backend_type == "obstore"
    assert backend.base_uri == "s3://bkt"


def test_fsspec_backend_methods(monkeypatch: "pytest.MonkeyPatch") -> None:
    from sqlspec.storage.backends.fsspec import FsspecBackend

    fs_mock = MagicMock()
    monkeypatch.setattr("sqlspec.storage.backends.fsspec.fsspec.filesystem", lambda protocol, **kwargs: fs_mock)
    backend = FsspecBackend("s3", {"bucket": "bkt"}, base_path="prefix")
    # read_bytes
    fs_mock.open().__enter__().read.return_value = b"data"
    assert backend.read_bytes("file") == b"data"
    # write_bytes
    backend.write_bytes("file", b"data")
    fs_mock.open().__enter__().write.assert_called_with(b"data")
    # read_text
    fs_mock.open().__enter__().read.return_value = b"txt"
    assert backend.read_text("file") == "txt"
    # write_text
    backend.write_text("file", "txt")
    fs_mock.open().__enter__().write.assert_called()
    # exists
    fs_mock.exists.return_value = True
    assert backend.exists("file") is True
    # delete
    backend.delete("file")
    fs_mock.rm.assert_called_with("prefix/file")
    # list_objects
    fs_mock.ls.return_value = ["a", "b"]
    assert backend.list_objects("pre") == ["a", "b"]  # type: ignore[attr-defined]
    # backend_type, base_uri
    assert backend.backend_type == "fsspec"
    assert backend.base_uri == "s3://bkt"


def test_localfile_backend_methods(tmp_path: "Path") -> None:
    from sqlspec.storage.backends.file import LocalFileBackend

    backend = LocalFileBackend(str(tmp_path))
    # write_bytes/read_bytes
    backend.write_bytes("foo.bin", b"abc")
    assert backend.read_bytes("foo.bin") == b"abc"
    # write_text/read_text
    backend.write_text("foo.txt", "hello")
    assert backend.read_text("foo.txt") == "hello"
    # exists
    assert backend.exists("foo.txt") is True
    # list_objects
    backend.write_text("bar.txt", "b")
    files = backend.list_objects()  # type: ignore[attr-defined]
    assert any("foo.txt" in f or "bar.txt" in f for f in files)
    # delete
    backend.delete("foo.txt")
    assert not backend.exists("foo.txt")
    # backend_type, base_uri
    assert backend.backend_type == "local"
    assert backend.base_uri.startswith("file://")
