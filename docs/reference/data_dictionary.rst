===============
Data Dictionary
===============

The data dictionary module provides schema introspection, table and column metadata,
foreign key resolution, dependency ordering, and database version capability profiling.

.. currentmodule:: sqlspec.data_dictionary

Metadata Types
==============

.. autoclass:: TableMetadata
   :members:
   :show-inheritance:

.. autoclass:: TableDetails
   :members:
   :show-inheritance:

.. autoclass:: ColumnMetadata
   :members:
   :show-inheritance:

.. autoclass:: ColumnDetails
   :members:
   :show-inheritance:

.. autoclass:: ForeignKeyMetadata
   :members:
   :show-inheritance:

.. autoclass:: IndexMetadata
   :members:
   :show-inheritance:

.. autoclass:: IndexDetails
   :members:
   :show-inheritance:

.. autoclass:: PartitionMetadata
   :members:
   :show-inheritance:

.. autoclass:: PrivilegeMetadata
   :members:
   :show-inheritance:

.. autoclass:: RoutineMetadata
   :members:
   :show-inheritance:

.. autoclass:: TriggerMetadata
   :members:
   :show-inheritance:

.. autoclass:: ViewMetadata
   :members:
   :show-inheritance:

Database Capability and Versions
================================

.. autoclass:: VersionInfo
   :members:
   :show-inheritance:

.. autoclass:: FeatureFlags
   :members:
   :show-inheritance:

.. autoclass:: FeatureVersions
   :members:
   :show-inheritance:

.. autoclass:: MetadataCapabilityProfile
   :members:
   :show-inheritance:

Data Dictionary Loader and Queries
==================================

.. autoclass:: DataDictionaryLoader
   :members:
   :show-inheritance:

.. autoclass:: MetadataQuery
   :members:
   :show-inheritance:

.. autoclass:: MetadataResult
   :members:
   :show-inheritance:

Helper Functions
================

.. autofunction:: get_data_dictionary_loader

.. autofunction:: get_dialect_config

.. autofunction:: register_dialect

.. autofunction:: list_registered_dialects

.. autofunction:: sort_dependencies
