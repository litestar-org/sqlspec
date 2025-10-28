==========
Quickstart
==========

This quickstart guide will get you running your first SQLSpec query in under 5 minutes. We'll use SQLite since it requires no additional setup, but the same patterns apply to all supported databases.

Your First Query
----------------

Let's start with the simplest possible example - executing a query and getting results:

.. literalinclude:: /examples/quickstart/quickstart_1.py
   :language: python
   :caption: ``first sqlspec query``

What's happening here?

1. **Create SQLSpec instance**: This is your central registry for database configurations.
2. **Configure a database**: Each database gets a config object. Here we're using in-memory SQLite.
3. **Get a session**: The context manager provides a database session and handles cleanup.
4. **Execute SQL**: Write your SQL directly - no magic, no abstraction.

Working with Real Data
----------------------

Let's create a table, insert some data, and query it:

.. literalinclude:: /examples/quickstart/quickstart_2.py
   :language: python
   :caption: ``working with real data``

Session Methods Cheat Sheet
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SQLSpec provides several convenient query methods:

- ``execute(sql, *params)``: Execute any SQL, returns full result
- ``select(sql, *params)``: Execute SELECT, returns list of rows
- ``select_one(sql, *params)``: Get single row, raises error if not found
- ``select_one_or_none(sql, *params)``: Get single row or None
- ``select_value(sql, *params)``: Get single scalar value
- ``execute_many(sql, params_list)``: Execute with multiple parameter sets
- ``execute_script(sql)``: Execute multiple SQL statements

Type-Safe Results
-----------------

The real power of SQLSpec comes from type-safe result mapping. Define your data models and SQLSpec automatically maps query results to them:

.. literalinclude:: /examples/quickstart/quickstart_3.py
   :language: python
   :caption: ``type-safe results``

.. note::

    SQLSpec supports multiple type libraries: Pydantic, msgspec, attrs, and standard library dataclasses. Choose the one that fits your project!

Async Support
-------------

SQLSpec supports async/await for non-blocking database operations. Here's the same example with async:

.. literalinclude:: /examples/quickstart/quickstart_4.py
   :language: python
   :caption: ``async support``

The API is identical - just add ``await`` and use async config/drivers!

Switching Databases
-------------------

One of SQLSpec's strengths is the consistent API across databases. Here's the same code using PostgreSQL:

.. literalinclude:: /examples/quickstart/quickstart_5.py
   :language: python
   :caption: ``switching databases``


.. tip::

    Each database has its own parameter style (``?`` for SQLite, ``$1`` for PostgreSQL, ``%s`` for MySQL, etc.). SQLSpec handles this automatically - you just need to use the correct style for your database.

Multiple Databases
------------------

Need to work with multiple databases? Register multiple configs:

.. code-block:: python

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec.adapters.duckdb import DuckDBConfig

    db_manager = SQLSpec()

    # Register multiple databases
    sqlite_db = db_manager.add_config(SqliteConfig(pool_config={"database": "app.db"}))
    duckdb_db = db_manager.add_config(DuckDBConfig(pool_config={"database": "analytics.duckdb"}))

    # Use different databases
    with db_manager.provide_session(sqlite_db) as sqlite_session:
        users = sqlite_session.select("SELECT * FROM users")

    with db_manager.provide_session(duckdb_db) as duckdb_session:
        analytics = duckdb_session.select("SELECT * FROM events")

Transaction Support
-------------------

SQLSpec automatically manages transactions. By default, each session is a transaction:

.. code-block:: python

    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": "mydb.db"}))

    # Transaction committed on successful exit
    with db_manager.provide_session(db) as session:
        session.execute("INSERT INTO users (name) VALUES (?)", "Alice")
        session.execute("INSERT INTO orders (user_name) VALUES (?)", "Alice")
        # Both committed together

    # Transaction rolled back on exception
    try:
        with db_manager.provide_session(db) as session:
            session.execute("INSERT INTO users (name) VALUES (?)", "Bob")
            raise ValueError("Something went wrong!")
    except ValueError:
        pass  # Transaction was rolled back automatically

.. note::

    Transaction behavior can be configured per session or globally. See the :doc:`../usage/drivers_and_querying` guide for details on transaction modes.

Query Builder (Experimental)
----------------------------

For those who prefer programmatic query construction, SQLSpec includes an experimental query builder:

.. code-block:: python

    from sqlspec import sql
    from sqlspec.adapters.sqlite import SqliteConfig
    from sqlspec import SQLSpec

    # Build a query programmatically
    query = (
        sql.select("id", "name", "email")
        .from_("users")
        .where("age > ?")
        .order_by("name")
    )

    db_manager = SQLSpec()
    db = db_manager.add_config(SqliteConfig(pool_config={"database": ":memory:"}))

    with db_manager.provide_session(db) as session:
        # Setup
        session.execute("""
            CREATE TABLE users (id INTEGER, name TEXT, email TEXT, age INTEGER)
        """)
        session.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?)",
            1, "Alice", "alice@example.com", 30
        )

        # Execute built query
        results = session.select(query, 25)
        print(results)

.. warning::

    The query builder API is experimental and will change significantly in future releases. Use raw SQL for production code.

Next Steps
----------

You've now seen the basics of SQLSpec! Here's where to go next:

**Usage Guides**

- :doc:`../usage/configuration` - Learn about configuration options and connection pooling
- :doc:`../usage/drivers_and_querying` - Deep dive into drivers, sessions, and query execution
- :doc:`../usage/data_flow` - Understand how SQLSpec processes queries internally
- :doc:`../usage/sql_files` - Load SQL queries from files
- :doc:`../usage/framework_integrations` - Integrate with Litestar, FastAPI, and Flask

**Examples**

- :doc:`../examples/index` - Gallery of practical examples for various databases

**API Reference**

- :doc:`../reference/index` - Complete API documentation

.. tip::

    SQLSpec is designed to be simple but powerful. Start with raw SQL and add features like type-safe mapping and the query builder only when you need them.
