=============
Query Builder
=============

SQLSpec includes an experimental fluent query builder API for programmatically constructing SQL queries. While raw SQL is recommended for most use cases, the query builder is useful for dynamic query construction.

.. warning::

   The Query Builder API is **experimental** and subject to significant changes. Use raw SQL for production-critical queries where API stability is required.

Overview
--------

The query builder provides a fluent, chainable API for constructing SQL statements:

.. literalinclude:: ../examples/usage/usage_query_builder_1.py
:caption: `Overview`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Why Use the Query Builder?
---------------------------

**Benefits**

- Type-safe query construction
- Reusable query components
- Dynamic filtering
- Protection against syntax errors
- IDE autocomplete support

**When to Use**

- Complex dynamic queries with conditional filters
- Query templates with variable components
- Programmatic query generation
- API query builders (search, filtering)

**When to Use Raw SQL Instead**

- Static, well-defined queries
- Complex joins and subqueries
- Database-specific features
- Performance-critical queries
- Queries loaded from SQL files

SELECT Queries
--------------

Basic SELECT
^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_2.py
:caption: `Basic SELECT`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



WHERE Clauses
^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_3.py
:caption: `WHERE Clauses`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Dynamic Filtering
^^^^^^^^^^^^^^^^^

Build queries conditionally based on runtime values:

.. literalinclude:: ../examples/usage/usage_query_builder_4.py
:caption: `Dynamic Filtering`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



JOINs
^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_5.py
:caption: `JOINs`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Ordering and Limiting
^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_6.py
:caption: `Ordering and Limiting`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Aggregations
^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_7.py
:caption: `Aggregations`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Subqueries
^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_8.py
:caption: `Subqueries`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



INSERT Queries
--------------

Basic INSERT
^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_9.py
:caption: `Basic INSERT`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Multiple Rows
^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_10.py
:caption: `Multiple Rows`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



INSERT with RETURNING
^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_11.py
:caption: `INSERT with RETURNING`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



UPDATE Queries
--------------

Basic UPDATE
^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_12.py
:caption: `Basic UPDATE`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Multiple Columns
^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_13.py
   :caption: `Multiple Columns`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Conditional Updates
^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_14.py
   :caption: `Conditional Updates`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



DELETE Queries
--------------

Basic DELETE
^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_15.py
   :caption: `Basic DELETE`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Multiple Conditions
^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_16.py
   :caption: `Multiple Conditions`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



DDL Operations
--------------

CREATE TABLE
^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_17.py
   :caption: `CREATE TABLE`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



DROP TABLE
^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_18.py
   :caption: `DROP TABLE`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



CREATE INDEX
^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_19.py
   :caption: `CREATE INDEX`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Advanced Features
-----------------

Window Functions
^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_20.py
   :caption: `Window Functions`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



CASE Expressions
^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_21.py
   :caption: `CASE Expressions`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Common Table Expressions (CTE)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_22.py
   :caption: `Common Table Expressions (CTE)`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Query Composition
-----------------

Reusable Query Components
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_23.py
   :caption: `Reusable Query Components`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Query Templates
^^^^^^^^^^^^^^^

.. literalinclude:: ../examples/usage/usage_query_builder_24.py
   :caption: `Query Templates`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Best Practices
--------------

**1. Use Raw SQL for Static Queries**

.. literalinclude:: ../examples/usage/usage_query_builder_25.py
   :caption: `1. Use Raw SQL for Static Queries`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



**2. Builder for Dynamic Queries**

.. literalinclude:: ../examples/usage/usage_query_builder_26.py
   :caption: `2. Builder for Dynamic Queries`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



**3. Parameterize User Input**

.. literalinclude:: ../examples/usage/usage_query_builder_27.py
   :caption: `3. Parameterize User Input`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



**4. Type Safety with Schema Mapping**

.. literalinclude:: ../examples/usage/usage_query_builder_28.py
   :caption: `4. Type Safety with Schema Mapping`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



**5. Test Generated SQL**

.. literalinclude:: ../examples/usage/usage_query_builder_29.py
   :caption: `5. Test Generated SQL`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Limitations
-----------

The query builder has some limitations:

**Complex Subqueries**

For very complex subqueries, raw SQL is often clearer:

.. literalinclude:: ../examples/usage/usage_query_builder_30.py
   :caption: `Complex Subqueries`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



**Database-Specific Features**

Database-specific syntax may not be supported:

.. literalinclude:: ../examples/usage/usage_query_builder_31.py
   :caption: `Database-Specific Features`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



**Performance**

The builder adds minimal overhead, but raw SQL is always fastest for known queries.

Migration from Raw SQL
----------------------

When migrating from raw SQL to the query builder:

.. literalinclude:: ../examples/usage/usage_query_builder_32.py
   :caption: `Migration from Raw SQL`
   :language: python
   :dedent:
   :start-after: # start-example
   :end-before: # end-example



Only migrate queries that benefit from dynamic construction.

Next Steps
----------

- :doc:`sql_files` - Load queries from SQL files (recommended for static queries)
- :doc:`drivers_and_querying` - Execute built queries with drivers
- :doc:`../reference/builder` - Complete query builder API reference

See Also
--------

- :doc:`data_flow` - Understanding query processing
- :doc:`configuration` - Configure statement processing
- :doc:`../examples/index` - Example queries and patterns
