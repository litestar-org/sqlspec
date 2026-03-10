=======
Storage
=======

Storage abstraction layer with multiple backend support (local filesystem,
fsspec, obstore), configuration-based registration, and Arrow table
import/export with CSV format support.

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
