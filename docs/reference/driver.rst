======
Driver
======

The driver module defines sync and async driver adapters, transaction helpers,
and the shared data dictionary mixins.

.. currentmodule:: sqlspec.driver

Example
=======

.. literalinclude:: /examples/reference/driver_api.py
   :language: python
   :caption: ``driver usage``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Base Driver Classes
===================

Synchronous Driver
------------------

.. autoclass:: SyncDriverAdapterBase
   :members:
   :undoc-members:
   :show-inheritance:

Asynchronous Driver
-------------------

.. autoclass:: AsyncDriverAdapterBase
   :members:
   :undoc-members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: DataDictionaryMixin
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncDataDictionaryBase
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SyncDataDictionaryBase
   :members:
   :undoc-members:
   :show-inheritance:

Feature Flag Types
==================

.. currentmodule:: sqlspec.data_dictionary

.. autoclass:: FeatureFlags
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: FeatureVersions
   :members:
   :undoc-members:
   :show-inheritance:

Driver Protocols
================

.. currentmodule:: sqlspec.protocols

.. autoclass:: DriverProtocol
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: AsyncDriverProtocol
   :members:
   :undoc-members:
   :show-inheritance:

.. autoclass:: SessionProtocol
   :members:
   :undoc-members:
   :show-inheritance:
