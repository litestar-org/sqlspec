Testing
=======

SQLSpec provides tools for both unit and integration testing of database code.

Pytest Fixture Tips
-------------------

- Use ``tmp_path`` (pytest built-in) for SQLite file databases
- Use ``:memory:`` for fast in-memory tests
- Create factory functions for reusable test setup
- Use ``execute_many`` for bulk fixture data
- Use ``select_value`` for assertion checks on counts

.. code-block:: python

    import pytest
    from sqlspec import SQLSpec
    from sqlspec.adapters.sqlite import SqliteConfig

    @pytest.fixture
    def db(tmp_path):
        spec = SQLSpec()
        config = spec.add_config(
            SqliteConfig(connection_config={"database": str(tmp_path / "test.db")})
        )
        with spec.provide_session(config) as session:
            session.execute("create table users (id integer primary key, name text)")
            yield session

Integration Test Patterns
-------------------------

For integration tests against real databases, use the standard ``SQLSpec`` +
adapter config pattern with temporary databases.

.. literalinclude:: /examples/patterns/integration_testing.py
   :language: python
   :caption: ``integration test fixtures``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Related Guides
--------------

- :doc:`configuration` for adapter configuration options.
- :doc:`drivers_and_querying` for the full query API.
