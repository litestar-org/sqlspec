Query Builder
=============

.. image:: /_static/demos/query_builder.gif
   :alt: SQLSpec query builder demo
   :class: demo-gif

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

Upserts (ON CONFLICT)
---------------------

Use ``.on_conflict()`` to handle insert conflicts. Chain ``.do_nothing()`` to skip
conflicting rows, or ``.do_update(**columns)`` to update them.

.. literalinclude:: /examples/builder/upsert.py
   :language: python
   :caption: ``upsert with on_conflict``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Raw Expressions and RETURNING
------------------------------

Use ``sql.raw()`` to embed raw SQL fragments (like database functions) inside
builder queries. Use ``.returning()`` on INSERT, UPDATE, or DELETE to get back
the affected rows.

.. literalinclude:: /examples/builder/raw_expressions.py
   :language: python
   :caption: ``raw expressions and returning``
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
