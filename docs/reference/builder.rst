=======
Builder
=======

The builder provides a fluent API for composing SQL statements with chaining,
filters, and dialect-aware SQL compilation.

.. currentmodule:: sqlspec.builder

.. warning::
   The builder API is **experimental** and subject to breaking changes.

Example
=======

.. literalinclude:: /examples/reference/builder_api.py
   :language: python
   :caption: ``builder usage``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Factory
=======

.. autoclass:: SQLFactory
   :members:
   :undoc-members:
   :show-inheritance:

Select Builder
==============

.. autoclass:: Select
   :members:
   :undoc-members:
   :show-inheritance:

More Examples
=============

- :doc:`/examples/index` for query modifiers and joins.
- :doc:`/usage/query_builder` for focused workflows.
