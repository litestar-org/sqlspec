=====================
Database Configuration
=====================

Base configuration classes for database adapters. All adapter-specific config
classes inherit from one of these bases.

.. currentmodule:: sqlspec.config

DatabaseConfigProtocol
======================

.. autoclass:: DatabaseConfigProtocol
   :members:
   :show-inheritance:

SyncDatabaseConfig
==================

.. autoclass:: SyncDatabaseConfig
   :members:
   :show-inheritance:

AsyncDatabaseConfig
===================

.. autoclass:: AsyncDatabaseConfig
   :members:
   :show-inheritance:

NoPoolSyncConfig
================

.. autoclass:: NoPoolSyncConfig
   :members:
   :show-inheritance:

NoPoolAsyncConfig
=================

.. autoclass:: NoPoolAsyncConfig
   :members:
   :show-inheritance:

Extension Configuration Types
==============================

.. autoclass:: LifecycleConfig
   :members:
   :show-inheritance:

.. autoclass:: MigrationConfig
   :members:
   :show-inheritance:

.. autoclass:: EventsConfig
   :members:
   :show-inheritance:

.. autoclass:: OpenTelemetryConfig
   :members:
   :show-inheritance:

.. autoclass:: PrometheusConfig
   :members:
   :show-inheritance:

.. autoclass:: ADKConfig
   :members:
   :show-inheritance:
