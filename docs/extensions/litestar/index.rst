===================
Litestar Extension
===================

SQLSpec ships a Litestar plugin that wires database configs into Litestar's
lifecycle and dependency injection.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   dependency_injection
   transactions
   session_stores
   api

Example
=======

.. literalinclude:: /examples/extensions/litestar/plugin_setup.py
   :language: python
   :caption: ``litestar plugin``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:
