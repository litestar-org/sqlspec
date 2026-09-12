=========
Data Flow
=========

SQLSpec processes database interactions through a four-stage pipeline: statement
authoring, compilation and normalization, driver execution, and result
transformation. This architecture decouples how queries are written from how
databases execute them and how applications consume results.

Pipeline Overview
-----------------

.. mermaid::

   flowchart TD
     A[Stage 1: Statement Input<br/>Raw SQL, SQL Object, Builder, or SQL Files] --> B[Stage 2: Compilation & Normalization<br/>StatementConfig, Placeholders, AST Cache, Declared Params]
     B --> C[Stage 3: Session & Driver Execution<br/>Sync / Async Driver Adapter, Connection Pool, Transactions]
     C --> D[Stage 4: Result Transformation<br/>SQLResult, Schema Mapping, Chunked Streams, Arrow Zero-Copy]

Stage 1: Statement Input
------------------------

SQLSpec accepts queries in multiple forms to match the needs of your application:

- **Raw SQL strings**: Plain SQL with named (``:name``) or positional (``?``) placeholders.
- **SQL objects**: Instances of :class:`~sqlspec.core.statement.SQL` with support for immutable ``.where()`` condition chaining, ``.select_only()``, and ``.paginate()``.
- **QueryBuilder**: The fluent AST builder (``sql.select()``, ``sql.insert()``, ``sql.update()``, ``sql.delete()``) with compile-time dialect targeting.
- **SQL Files**: External ``.sql`` files loaded into the registry via ``spec.load_sql_files()`` and retrieved by name with ``spec.get_sql()``.

Stage 2: Compilation & Normalization
-------------------------------------

Before reaching the database, queries pass through the core pipeline:

- **Parameter style conversion**: Placeholders (``:param``, ``?``, ``$n``) are automatically mapped to the dialect-native style required by the target driver.
- **Declared parameter validation**: Queries loaded from SQL files with ``-- param:`` directives have parameter presence and Python types verified before driver dispatch.
- **Compiled statement caching**: Parameterized queries and AST structures are cached by fingerprint to bypass repeated SQL parsing overhead on hot paths.
- **Dialect translation**: SQLGlot-powered AST transformers can convert expressions between SQL dialects when cross-database portability is needed.

Stage 3: Session & Driver Execution
-----------------------------------

Database operations occur within a managed session:

- **Session provisioning**: Calling ``spec.provide_session(config)`` yields a synchronous (``SyncDriverAdapterBase``) or asynchronous (``AsyncDriverAdapterBase``) driver adapter.
- **Connection management**: The session acquires a physical connection from the config's pool (``provide_pool()`` / ``create_pool()``) and releases it upon exit.
- **Transaction control**: Sessions manage transaction boundaries via ``session.begin()``, ``session.commit()``, ``session.rollback()``, savepoints, or the ``service.begin_transaction()`` context manager.
- **Exception mapping**: Low-level database errors (DBAPI exceptions, driver errors) are translated into unified SQLSpec exceptions (:exc:`~sqlspec.exceptions.SQLSpecError`, :exc:`~sqlspec.exceptions.IntegrityError`, :exc:`~sqlspec.exceptions.OperationalError`).

Stage 4: Result Transformation
------------------------------

Drivers provide flexible consumption patterns for query results:

- **Structured SQLResult**: Low-level ``session.execute()`` returns a :class:`~sqlspec.core.result.SQLResult` with ``.all()``, ``.one()``, ``.one_or_none()``, ``.scalar()``, ``.scalar_or_none()``, and DML stats (``.rows_affected``, ``.last_inserted_id``).
- **Direct query helpers**: ``session.select()``, ``session.select_one()``, ``session.select_value()``, and ``session.select_with_total()`` return processed rows directly.
- **Schema mapping**: Passing ``schema_type=Model`` automatically maps result rows into dataclasses, msgspec Structs, Pydantic models, attrs classes, or TypedDict instances.
- **Memory-bounded streaming**: ``session.select_stream(chunk_size=N)`` streams large result sets in bounded chunks using the driver's native cursor streaming primitive.
- **Arrow integration**: ``session.select_to_arrow()`` yields Apache Arrow Tables, RecordBatches, or RecordBatchReaders for zero-copy analytical processing.

Minimal Execution Example
-------------------------

.. literalinclude:: /examples/querying/execute_select.py
   :language: python
   :caption: ``execute select``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Related Guides
--------------

- :doc:`configuration` for configuring adapters, pools, and multiple databases.
- :doc:`drivers_and_querying` for driver execution APIs, streaming, and transactions.
- :doc:`query_builder` for the fluent query builder API.
- :doc:`sql_files` for organizing SQL files and parameter declarations.
