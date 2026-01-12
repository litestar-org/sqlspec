=======================
SQLSpec Example Library
=======================

This catalog mirrors the primary SQLSpec workflows. Each snippet focuses on a single
idea, stays under 75 lines, and can be executed via pytest.

Quickstart
==========

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``quickstart/basic_connection.py``
     - Create a registry, open a session, and run a simple query.
   * - ``quickstart/first_query.py``
     - Insert and read back a row.
   * - ``quickstart/configuration.py``
     - Configure statement behavior and verify execution.

Frameworks
==========

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``frameworks/litestar/basic_setup.py``
     - Register the SQLSpec plugin in Litestar.
   * - ``frameworks/litestar/dependency_injection.py``
     - Customize Litestar session dependency keys.
   * - ``frameworks/litestar/commit_modes.py``
     - Configure autocommit vs manual commit behavior.
   * - ``frameworks/litestar/session_stores.py``
     - Use SQLSpec-backed session stores.
   * - ``frameworks/fastapi/basic_setup.py``
     - FastAPI dependency injection with SQLSpec.
   * - ``frameworks/flask/basic_setup.py``
     - Flask extension setup and session access.
   * - ``frameworks/starlette/basic_setup.py``
     - Starlette plugin setup with async sessions.

Drivers
=======

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``drivers/sqlite_connection.py``
     - Configure and run a SQLite session.
   * - ``drivers/asyncpg_connection.py``
     - AsyncPG configuration example.
   * - ``drivers/cockroach_asyncpg_connection.py``
     - CockroachDB config using AsyncPG.
   * - ``drivers/cockroach_psycopg_connection.py``
     - CockroachDB config using Psycopg.
   * - ``drivers/mysqlconnector_connection.py``
     - MySQL connector configuration.
   * - ``drivers/pymysql_connection.py``
     - PyMySQL configuration.
   * - ``drivers/transaction_handling.py``
     - Manual transactions with begin/commit/rollback.
   * - ``drivers/parameter_binding.py``
     - Named and positional parameter binding.

Querying
========

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``querying/execute_select.py``
     - Basic SELECT execution.
   * - ``querying/execute_insert.py``
     - INSERT execution with row counts.
   * - ``querying/batch_operations.py``
     - Execute batches with execute_many.
   * - ``querying/statement_stack.py``
     - Execute multi-statement stacks.

SQL Files
=========

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``sql_files/load_sql_files.py``
     - Load SQL from files with named queries.
   * - ``sql_files/named_queries.py``
     - Register named SQL directly in the registry.

Builder
=======

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``builder/select_query.py``
     - Build a SELECT with the fluent builder.
   * - ``builder/insert_query.py``
     - Build an INSERT statement.
   * - ``builder/update_query.py``
     - Build an UPDATE statement.
   * - ``builder/complex_joins.py``
     - Join across multiple tables.
   * - ``builder/query_modifiers.py``
     - Use where helpers, pagination, and select_only.

Extensions
==========

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``extensions/litestar/plugin_setup.py``
     - Configure Litestar extension settings.
   * - ``extensions/litestar/dependency_keys.py``
     - Multiple Litestar dependency keys.
   * - ``extensions/litestar/multiple_databases.py``
     - Multi-database registry setup.
   * - ``extensions/adk/memory_store.py``
     - ADK session store workflow.
   * - ``extensions/adk/tool_integration.py``
     - ADK memory store and search workflow.
   * - ``extensions/adk/backend_config.py``
     - ADBC + GizmoSQL backend configuration.

Observability
=============

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``patterns/observability/correlation_middleware.py``
     - Correlation context example.
   * - ``patterns/observability/sampling_config.py``
     - Sampling configuration.
   * - ``patterns/observability/cloud_formatters.py``
     - Cloud log formatters.

Reference
=========

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``reference/base_api.py``
     - SQLSpec registry APIs.
   * - ``reference/core_api.py``
     - SQL and result APIs.
   * - ``reference/driver_api.py``
     - Driver execution APIs.
   * - ``reference/builder_api.py``
     - Builder entry points.

Contributing
============

.. list-table::
   :header-rows: 1

   * - File
     - Description
   * - ``contributing/new_adapter.py``
     - Adapter skeleton example.

.. toctree::
   :hidden:

   quickstart/basic_connection
   quickstart/first_query
   quickstart/configuration
   frameworks/litestar/basic_setup
   frameworks/litestar/dependency_injection
   frameworks/litestar/commit_modes
   frameworks/litestar/session_stores
   frameworks/fastapi/basic_setup
   frameworks/flask/basic_setup
   frameworks/starlette/basic_setup
   drivers/asyncpg_connection
   drivers/cockroach_asyncpg_connection
   drivers/cockroach_psycopg_connection
   drivers/mysqlconnector_connection
   drivers/pymysql_connection
   drivers/sqlite_connection
   drivers/transaction_handling
   drivers/parameter_binding
   querying/execute_select
   querying/execute_insert
   querying/batch_operations
   querying/statement_stack
   sql_files/load_sql_files
   sql_files/named_queries
   builder/select_query
   builder/insert_query
   builder/update_query
   builder/complex_joins
   builder/query_modifiers
   extensions/litestar/plugin_setup
   extensions/litestar/dependency_keys
   extensions/litestar/multiple_databases
   extensions/adk/memory_store
   extensions/adk/tool_integration
   extensions/adk/backend_config
   patterns/observability/correlation_middleware
   patterns/observability/sampling_config
   patterns/observability/cloud_formatters
   reference/base_api
   reference/core_api
   reference/driver_api
   reference/builder_api
   contributing/new_adapter
