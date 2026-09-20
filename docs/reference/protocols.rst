=========
Protocols
=========

Protocol definitions and runtime-checkable interfaces used across SQLSpec's
core, drivers, builder, and data dictionary.

.. currentmodule:: sqlspec.protocols

Statement and Query Protocols
=============================

.. autoclass:: StatementProtocol
   :members:
   :show-inheritance:

.. autoclass:: SQLBuilderProtocol
   :members:
   :show-inheritance:

.. autoclass:: QueryResultProtocol
   :members:
   :show-inheritance:

.. autoclass:: PipelineCapableProtocol
   :members:
   :show-inheritance:

.. autoclass:: SupportsArrowResults
   :members:
   :show-inheritance:

Data Dictionary Protocols
=========================

.. autoclass:: AsyncDataDictionaryProtocol
   :members:
   :show-inheritance:

.. autoclass:: SyncDataDictionaryProtocol
   :members:
   :show-inheritance:

Storage and Driver Protocols
============================

.. autoclass:: ObjectStoreProtocol
   :members:
   :show-inheritance:

.. autoclass:: SupportsCloseProtocol
   :members:
   :show-inheritance:

.. autoclass:: NotificationProtocol
   :members:
   :show-inheritance:

.. autoclass:: MappingLikeProtocol
   :members:
   :show-inheritance:
