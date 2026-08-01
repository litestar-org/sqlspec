=======
Storage
=======

Storage abstraction layer with multiple backend support (local filesystem,
fsspec, obstore), configuration-based registration, and Arrow table
import/export with CSV format support.

Write and read formats
======================

Row-oriented writes accept only ``json`` and newline-delimited ``jsonl``.
Arrow-table writes accept only ``parquet``, ``arrow-ipc``, and ``csv``. The
pipeline rejects mismatched formats before encoding or storage I/O, so it
cannot write one payload type under another format label. Read APIs retain the
full format set because they decode all five formats into Arrow tables.

JSONL reads use PyArrow's native JSON reader. Its type inference applies to the
result, including conversion of date-like strings to Arrow timestamps. This
avoids Python per-line decoding and ``Table.from_pylist()`` copies. It does not
make ``load_from_storage()`` bounded-memory: that API reads the complete object
payload before decoding it.

Pipelines
=========

.. autoclass:: sqlspec.storage.SyncStoragePipeline
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.AsyncStoragePipeline
   :members:
   :show-inheritance:

Registry
========

.. autoclass:: sqlspec.storage.StorageRegistry
   :members:
   :show-inheritance:

Configuration Types
===================

.. autoclass:: sqlspec.storage.StorageCapabilities
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.PartitionStrategyConfig
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.StorageLoadRequest
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.StagedArtifact
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.StorageTelemetry
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.StorageBridgeJob
   :members:
   :show-inheritance:

Backends
========

.. autoclass:: sqlspec.storage.backends.base.ObjectStoreBase
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.backends.local.LocalStore
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.backends.fsspec.FSSpecBackend
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.storage.backends.obstore.ObStoreBackend
   :members:
   :show-inheritance:

Module Functions
================

.. autofunction:: sqlspec.storage.create_storage_bridge_job

.. autofunction:: sqlspec.storage.get_storage_bridge_diagnostics

.. autofunction:: sqlspec.storage.get_storage_bridge_metrics

.. autofunction:: sqlspec.storage.reset_storage_bridge_metrics

.. autofunction:: sqlspec.storage.resolve_storage_path
