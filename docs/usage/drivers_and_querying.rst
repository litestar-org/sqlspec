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
