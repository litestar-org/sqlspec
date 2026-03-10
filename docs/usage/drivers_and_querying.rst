Drivers and Querying
====================

SQLSpec provides a unified driver interface across supported databases. This page
covers the common execution patterns and points to adapter-specific configuration.

Supported Drivers (High Level)
------------------------------

- **PostgreSQL**: asyncpg, psycopg (sync/async), psqlpy, ADBC
- **SQLite**: sqlite3, aiosqlite, ADBC
- **MySQL**: asyncmy, mysql-connector, pymysql
- **Analytics / Cloud**: DuckDB, BigQuery, Spanner, Oracle, ADBC

Core Execution Pattern
----------------------

.. literalinclude:: /examples/drivers/sqlite_connection.py
   :language: python
   :caption: ``sqlite session``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Transactions
------------

.. literalinclude:: /examples/drivers/transaction_handling.py
   :language: python
   :caption: ``manual transaction``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Parameter Binding
-----------------

.. literalinclude:: /examples/drivers/parameter_binding.py
   :language: python
   :caption: ``positional and named parameters``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Schema Mapping
--------------

Use the ``schema_type`` parameter on ``select()``, ``select_one()``, and
``select_one_or_none()`` to map result rows to dataclass instances automatically.

.. literalinclude:: /examples/querying/schema_mapping.py
   :language: python
   :caption: ``schema_type mapping``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Scalar Values and Pagination
-----------------------------

``select_value`` returns a single scalar from a one-row, one-column result.
``select_value_or_none`` returns ``None`` when no rows match.
``select_with_total`` returns both the data page and the total count for pagination.

.. literalinclude:: /examples/querying/batch_operations.py
   :language: python
   :caption: ``scalar values, execute_many, and pagination``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Statement Stacks
----------------

Statement stacks bundle multiple SQL statements plus parameter sets. Drivers that
support native pipelines or batch execution can send the stack in a single round
trip, while others execute each statement sequentially.

.. literalinclude:: /examples/querying/statement_stack.py
   :language: python
   :caption: ``statement stack``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Driver Configuration Examples
-----------------------------

.. literalinclude:: /examples/drivers/asyncpg_connection.py
   :language: python
   :caption: ``asyncpg config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

.. literalinclude:: /examples/drivers/cockroach_psycopg_connection.py
   :language: python
   :caption: ``cockroach + psycopg``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

.. literalinclude:: /examples/drivers/mysqlconnector_connection.py
   :language: python
   :caption: ``mysql connector config``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Related References
------------------

- :doc:`../reference/adapters` for full adapter configuration reference.
- :doc:`/reference/adapters` for adapter capabilities and connection profiles.
