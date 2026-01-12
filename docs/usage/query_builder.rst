Query Builder
=============

SQLSpec includes a fluent query builder for teams who prefer structured SQL construction.
The builder outputs ``SQL`` objects that can be executed with the same driver APIs.

Selects
-------

.. literalinclude:: /examples/builder/select_query.py
   :language: python
   :caption: ``select query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Inserts and Updates
-------------------

.. literalinclude:: /examples/builder/insert_query.py
   :language: python
   :caption: ``insert query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

.. literalinclude:: /examples/builder/update_query.py
   :language: python
   :caption: ``update query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Joins
-----

.. literalinclude:: /examples/builder/complex_joins.py
   :language: python
   :caption: ``join query``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Query Modifiers
---------------

.. literalinclude:: /examples/builder/query_modifiers.py
   :language: python
   :caption: ``where helpers + pagination``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Related Guides
--------------

- :doc:`drivers_and_querying` for execution behavior.
- :doc:`../reference/builder` for the full builder API.
