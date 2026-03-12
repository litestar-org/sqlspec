=======
Spanner
=======

Google Cloud Spanner adapter using the Spanner client library with session
pool management.

Configuration
=============

.. autoclass:: sqlspec.adapters.spanner.SpannerSyncConfig
   :members:
   :show-inheritance:

Custom Dialects
================

Spanner uses the :doc:`Spanner and Spangres dialects <../dialects>` for SQL compilation.
See the :doc:`Dialects <../dialects>` reference for details.

Driver
======

.. autoclass:: sqlspec.adapters.spanner.SpannerSyncDriver
   :members:
   :show-inheritance:
