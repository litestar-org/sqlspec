from sqlspec.storage.registry import storage_registry

# Register LocalFileBackend
try:
    from sqlspec.storage.backends.file import LocalFileBackend

    storage_registry.register_backend("file", LocalFileBackend)
except ImportError:
    pass

# Register ObstoreBackend
try:
    from sqlspec.storage.backends.obstore import ObstoreBackend

    storage_registry.register_backend("s3", ObstoreBackend)
    storage_registry.register_backend("gs", ObstoreBackend)
    storage_registry.register_backend("az", ObstoreBackend)
except ImportError:
    pass

# Register FsspecBackend
try:
    from sqlspec.storage.backends.fsspec import FsspecBackend

    storage_registry.register_backend("fsspec", FsspecBackend)
except ImportError:
    pass
