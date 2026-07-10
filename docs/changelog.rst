=========
Changelog
=========

All notable SQLSpec changes are summarized here. Entries are grouped by release
and focus on user-visible behavior, public API changes, compatibility notes, and
important operational fixes.

Recent Updates
==============

Unreleased
------------------------------------------------------------------------------

**Breaking changes:**

* Began replacing the old narrow data-dictionary interface with a consistent
  metadata contract based on ``MetadataCapabilityProfile``,
  ``MetadataCapability``, ``MetadataResult``, ``ObjectIdentity``, and
  ``DDLResult``. This is a pre-1.0 breaking change: structural domain lookups
  return result envelopes, object DDL lookups return ``DDLResult`` directly, and
  callers should inspect capability or DDL status instead of treating empty
  lists as unsupported metadata.
* Standardized event transport configuration on ``notify``, ``notify_queue``,
  ``poll_queue``, ``aq``, and ``txeventq``. Retired transport names now raise
  an explicit configuration error with the canonical replacement.

**Added:**

* Added sync and async event-channel ``publish_many()`` APIs. Batch-capable
  implementations preserve input order and publish a grouped call in one
  transaction; custom backends retain an ordered single-event fallback.
* Added ``event_poll_interval`` for durable event reconciliation, independently
  of native listener wakeups. ``poll_interval`` remains a compatibility input.

**Changed:**

* PostgreSQL listeners now hold one dedicated long-lived connection while
  publishers use short pooled sessions. Native PostgreSQL batch publication
  reuses one publisher transaction per grouped call.
* ``notify_queue`` batch publication now bulk-inserts durable rows and sends one
  compact wakeup marker per channel rather than one notification per event.

**Fixed:**

* Event channels now honor adapter ``events_backend`` driver features when no
  extension-level backend is configured.
* Early mysql-connector row-stream cleanup now consumes unread results without
  reconnecting underneath an active transaction.
* Public row streams continue to clean up duck-typed sources whose ``close()``
  method uses the original no-argument contract.
* Durable notification queues now drain all rows represented by a batch marker,
  suppress duplicate markers, and recover missed markers through periodic
  durable reconciliation.
* Durable batch publication now preserves input delivery order, rolls back row
  inserts when marker publication fails, and drains all recovered rows after a
  lost marker without another native wait per event.
* Event listener shutdown now cancels async waits concurrently and bounds sync
  thread joins. Empty table queues do not poll faster than
  ``event_poll_interval``.

**Docs:**

* Expanded the data dictionary guide with capability vocabulary, support
  matrix, DDL/dependency guidance, and safe system-metadata opt-in behavior.
* Documented event transport delivery semantics, adapter support, connection
  ownership, batch behavior, and polling recovery.

v0.54.0 - SQL processing correctness and cleanup
------------------------------------------------------------------------------

**Changed:**

* Standardized adapter ``create_mapped_exception()`` helper signatures to accept
  ``(error, *, logger=None)`` across backends while preserving existing
  exception mapping behavior.
* Standardized adapter ``apply_driver_features()`` helpers to return an updated
  statement config plus normalized driver-feature dictionary across backends.
* MySQL-family adapter config, driver, and pool modules now resolve runtime
  vendor symbols through adapter-local typing modules.
* Oracle LOB fetches now default to direct string/byte materialization where
  python-oracledb supports it. Pass ``fetch_lobs=True`` when application code
  needs native Oracle LOB locators. Unconstrained LOB contents are no longer
  parsed with content heuristics; native ``JSON``, ``IS JSON`` CLOB/BLOB, and
  OSON-capable values still decode through Oracle JSON metadata.
* Driver statement-object caches are now bounded by the configured statement
  cache size, and cached named-parameter rebinding reuses driver-owned
  processing state.
* MySQL local-infile support now requires explicit opt-in consent before
  enabling client-side file reads.
* Removed unused private builder, driver, compiler, cache, parameter,
  SQL-file loader, storage, ADK, migration, and adapter internals while
  preserving public imports and compatibility surfaces.

**Fixed:**

* Dynamic SQLCommenter context and trace attributes are appended after stable
  SQL compilation, so repeated compiles reuse cached uncommented SQL while still
  using the current request context.
* Statement configs are frozen before pipeline fingerprinting so repeated
  compiles avoid avoidable cache-key hashing.
* Repeated statement-cache stores now skip redundant processed-state cloning
  when the raw SQL is already cached.
* Parameter extraction, type-dispatch misses, scalar coercion, and execute-many
  fingerprints now avoid unnecessary hashing and allocation on hot paths.
* No-op AST transformers no longer force full SQL finalization when they return
  the original expression and parameter objects.
* Simple dict and keyword-parameter executions can use the direct statement
  cache path when the cached query profile can safely rebind them.
* Oracle lock-target rendering for builder-generated ``FOR UPDATE OF`` clauses
  is handled by SQLGlot generation rather than post-render SQL rewriting.
* ``adbc`` and ``arrow-odbc`` configs now honor ``on_connection_create`` driver
  hooks after creating raw connections.
* CockroachDB psycopg session contexts now resolve callable statement configs
  at session entry, matching the rest of the PostgreSQL family.
* Bridge cursor cleanup now suppresses close failures consistently so cleanup
  errors do not mask an in-flight database exception.
* The ``mssql-python`` connection pool implementation now lives in its adapter
  pool module while preserving the existing public import.
* ``mssql_python`` stack execution no longer raises the base
  ``_connection_in_transaction()`` error before applying batched statements.
* ``arrow-odbc`` SQL Server transactions now rely on the connection
  commit/rollback API instead of sending a raw ``BEGIN TRANSACTION`` statement,
  so committed DML remains visible to later sessions.
* Spanner adapter modules no longer expose module-level proxy lookup hooks.
* Async migration squash now builds its internal migration runner with a real
  migration context, matching the synchronous command path.
* ObStore Arrow streaming no longer resolves cloud ``base_path`` twice for
  async streams.
* ``sql.decode()`` now renders a trailing default argument as the ``ELSE``
  clause documented for DECODE-style expressions.
* Async drivers can use the statement-cache direct execution path when the
  cursor supports awaitable ``execute()``, activating adapter row and rowcount
  hooks that were previously bypassed.
* aiomysql ADK table DDL now honors generated event columns, covering indexes,
  and adapter-local MySQL table options.
* MySQL-family ADK stores now recognize missing-table errors reported through
  an ``errno`` attribute as well as positional error arguments.
* Count-query generation no longer infers a missing outer ``FROM`` from tables
  nested inside scalar subqueries.
* Data dictionary default driver features now come from the dialect-specific
  mixin instead of being hidden by the generic compatibility mixin.
* Explicit ``optimize_expression=True`` now overrides a builder created with
  optimization disabled.
* ``where_in()`` now binds plain string values as scalar parameters, matching
  ``where_not_in()`` and the OR helper variants.
* Documentation builds now filter the known ``pymssql`` stub-only
  ``QueryParams`` guarded-import warning through the custom Sphinx tooling
  instead of changing adapter runtime code.

v0.52.0 - SQL Server adapters, ADK profiles, and cloud connectors
------------------------------------------------------------------------------

**Added:**

* Added the sync ``pymssql`` SQL Server adapter with config, driver, connection
  pool, data dictionary, migrations, event-store, Litestar session-store, and
  ADK store support.
* Added SQL Server support for ``arrow_odbc`` adapter contracts, ADK
  session/event storage, event queue storage, and Litestar session storage.
* Added ADK store and tuning profiles across SQLite, DuckDB, PostgreSQL,
  CockroachDB, MySQL, BigQuery, Spanner, ``mssql-python``, and ``arrow_odbc``.
  These profiles expose adapter-local table, index, full-text search, retention,
  and backend-specific DDL options.
* Added Google Cloud connector support for sync adapters: Cloud SQL for
  ``pymysql`` and AlloyDB for sync ``psycopg``.
* Added native Oracle event backends for Advanced Queuing and Transactional
  Event Queues.
* Added row-locking capability introspection across data dictionary dialects.
* Added docs coverage for the new SQL Server adapters, cloud connector setup,
  ADK backend matrix entries, and package extras parity.

**Changed:**

* Standardized Oracle native event backend names to ``aq`` and ``txeventq``;
  ``poll_queue`` remains the default backend.
* Moved ADK optimization and storage tuning options into adapter-local config
  types instead of the shared global config surface.
* Tightened adapter typing and core pipeline internals for the compiler,
  splitter, parameter handling, filters, result handling, cache/runtime helpers,
  and mypyc-ready adapter boundaries.
* Updated package extras to include current adapter and framework integrations,
  including ``arrow-odbc``, ``mssql-python``, ``pymssql``, ``sanic``, and
  ``starlette``.

**Fixed:**

* Removed the obsolete ``aioodbc`` extra and added docs/package parity checks so
  the installation guide matches available extras.
* Corrected ``pymysql`` stack transaction-state detection so nested stack
  execution reflects the real driver transaction state.
* Localized ADK optimization config to adapter implementations so backend
  tuning no longer depends on unused shared config keys.

v0.51.0 - ADK 2.0 clean-break store contract
------------------------------------------------------------------------------

**Breaking changes:**

* The ADK session and event store contract is rebuilt for Google ADK 2.0
  (verified through ``google-adk`` 2.3.0). Sessions are now keyed by
  ``(app_name, user_id, session_id)`` across every adapter, and the session
  service APIs (``create_session``, ``get_session``, ``list_sessions``,
  ``delete_session``) are keyword-only.
* ``get_session()``, ``delete_session()``, and ``update_session_state()`` on
  the store now require ``app_name`` and ``user_id`` in addition to
  ``session_id``. ``update_session_state(app_name, user_id, session_id,
  state)`` replaces the former two-argument form.
* The event payload column was renamed from ``event_json`` to ``event_data``
  on every ADK adapter store.
* Session state is split into scoped tables. Alongside ``adk_session`` and
  ``adk_event``, stores now manage ``adk_app_state``, ``adk_user_state``, and
  ``adk_internal_metadata``.
* Migration ``0002_reset_adk_tables`` is destructive: it unconditionally drops
  legacy ADK tables (sessions, events, app/user state, metadata, memory) and
  recreates them in the 2.0 shape. Back up ADK data before upgrading.
* ``sqlspec.utils.sync_tools.async_()`` now uses SQLSpec's managed
  ``ThreadPoolExecutor`` by default instead of delegating to the event loop's
  default executor through ``asyncio.to_thread()``. Configure the worker limit
  with ``SQLSPEC_ASYNC_THREAD_LIMIT`` or
  ``enable_default_async_thread_pool()``.

**Added:**

* Typed environment parsing helpers in ``sqlspec.utils.env``.
* ``ThreadPoolExecutor`` support for ``sqlspec.utils.sync_tools.async_()``, plus
  bounded async bridge controls through
  ``SQLSPEC_ASYNC_THREAD_LIMIT``, ``enable_default_async_thread_pool()``,
  ``set_default_async_executor()``, ``get_default_async_executor()``, and
  ``shutdown_default_async_executor()``.
* Scoped-state accessors on every ADK store: ``get_app_state``,
  ``get_user_state``, ``upsert_app_state``, ``upsert_user_state``,
  ``get_metadata``, and ``set_metadata``.
* ``append_event_and_update_state()`` accepts optional ``app_state`` and
  ``user_state`` deltas and applies them atomically with the session and event
  write, returning the updated ``SessionRecord``.

**Fixed:**

* Preserved ``contextvars`` when ``async_()`` routes sync work through explicit
  or shared thread executors.
* Removed a dead ``storage_uri`` key from the artifact-store config
  normalization; the artifact storage URI is supplied to ``ADKArtifactService``
  through its constructor and was never read from the store config.
* ``DMLResult.all()`` and ``one_or_none()`` no longer raise ``AttributeError``
  when called with ``schema_type``; the fast DML result path now initializes its
  schema-row caches.
* ``SQLProcessor.clear_cache()`` now resets the single-entry micro-cache, so the
  next compile of a previously compiled statement is recorded as a miss and
  repopulates the cache instead of returning a stale result.
* The SQL statement splitter caches results on the script text rather than
  ``hash(sql)``, preventing a hash collision between two distinct scripts from
  returning the wrong split.
* ``hash_parameters`` no longer raises ``TypeError`` for named parameters with
  unhashable values (for example ``set`` or ``bytearray``); such values now fall
  back to a stable ``repr``-based key, matching the positional path.

v0.50.1 - DuckDB extension lifecycle and SQLGlot builder modernization
------------------------------------------------------------------------------

**Changed:**

* Modernized the SQLGlot builder code paths.

**Fixed:**

* Separated DuckDB extension installation from loading with a best-effort
  lifecycle, so a failing optional extension no longer aborts connection setup.

v0.50.0 - Adapter config modernization, row streaming, and fetch tuning
------------------------------------------------------------------------------

**Added:**

* Native row streaming via ``select_stream()`` across all adapters, built on a
  new Arrow-streaming foundation with Arrow-native streaming paths.
* Driver-level cache and fetch tuning controls.
* SQLite runtime connection setup.
* Oracle sparse ``VECTOR`` passthrough.
* SQL-file parameter metadata annotations (``-- param:``).

**Changed:**

* Modernized adapter configuration across the full adapter suite: sqlite,
  aiosqlite, asyncpg, psycopg, psqlpy, oracledb, duckdb, asyncmy, aiomysql,
  mysqlconnector, pymysql, adbc, arrow-odbc, bigquery, spanner, mssql, and the
  cockroach (asyncpg/psycopg) configs.

**Fixed:**

* Honored optimizer flags in the query builder.
* Preserved the ADBC driver-manager configuration.

v0.49.1 - Transaction context-manager propagation
------------------------------------------------------------------------------

**Fixed:**

* ``begin_transaction`` context managers no longer suppress exceptions raised
  inside the block.

v0.49.0 - Driver-contract matrix consolidation
------------------------------------------------------------------------------

**Changed:**

* Consolidated the adapter suite into a shared driver-contract test matrix as
  part of a mypyc and code-quality overhaul.

**Fixed:**

* Normalized dialect identifier bindings.
* Generated Oracle-safe parameter names.

v0.48.2 - Filter-provider deepcopy fix
------------------------------------------------------------------------------

**Fixed:**

* Dropped the filter-provider modules from mypyc compilation to restore
  ``copy.deepcopy`` support for providers.

v0.48.1 - deepcopy and pickle for compiled value objects
------------------------------------------------------------------------------

**Fixed:**

* Supported ``copy.deepcopy`` and ``pickle`` on mypyc-compiled value objects.

v0.48.0 - Arrow ODBC and mssql-python adapters, migration schemas
------------------------------------------------------------------------------

**Added:**

* New ``arrow_odbc`` and ``mssql_python`` adapters.
* Support for specifying a schema for migrations.

**Fixed:**

* Repaired filter providers and adapter regressions.

v0.47.0 - Persistent listeners, schema builders, and performance polish
------------------------------------------------------------------------------

**Breaking changes:**

* ``schema_dump()``, ``serialize_collection()``, and
  ``get_collection_serializer()`` now default ``wire_format=False``. Msgspec
  structs with ``rename=`` now emit Python attribute names by default, matching
  Pydantic, dataclasses, and attrs. Pass ``wire_format=True`` to keep
  wire-aligned names.
* Third-party ADK stores implementing ``append_event_and_update_state()`` must
  return the updated ``SessionRecord``.
* Data dictionary metadata/version helpers now live under
  ``sqlspec.data_dictionary``. ``ColumnMetadata``, ``ForeignKeyMetadata``,
  ``IndexMetadata``, ``TableMetadata``, ``VersionInfo``, and
  ``VersionCacheResult`` are no longer exported from ``sqlspec.typing`` or
  ``sqlspec.core``.
* Removed modernization compatibility shims and deprecated helpers. Use
  ``SQL.raw_sql`` instead of ``SQL.sql``,
  ``CorrelationContext.context()`` instead of
  ``sqlspec.utils.correlation.correlation_context()``,
  ``MSSQL_CONFIG.default_schema`` instead of
  ``resolve_mssql_default_schema()``, ``Insert.values_from()`` and
  ``Insert.values_from_many()`` instead of ``Insert.values_from_dict()`` and
  ``Insert.values_from_dicts()``, ``clear_all_caches()`` or
  ``reset_stats_only()`` instead of ``reset_cache_stats()`` or
  ``SQLSpec.reset_cache_stats()``, and ``len(cache)`` instead of
  ``LRUCache.size()``. Oracle session callbacks are now always installed, so
  ``requires_session_callback()`` was removed.
* Removed filter compatibility APIs. ``PaginationFilter`` and
  ``create_filters()`` are gone, ``LimitOffsetFilter`` now subclasses
  ``StatementFilter``, and ``OrderByFilter`` rejects invalid ``sort_order``
  values instead of silently coercing them to ``asc``.
* Tightened parameter and serializer helpers. ``ParameterStyleConfig.hash()``
  was removed in favor of ``hash(config)``.
  ``build_null_pruning_transform()`` and
  ``replace_null_parameters_with_literals()`` no longer accept ``validator=``
  and require an explicit ``parameter_profile`` for non-empty parameter sets.
  ``build_time_iso_converter()`` was replaced by the shared
  ``time_iso_convert`` helper.
* Operation/result semantics changed. ``OperationType`` no longer includes
  ``UNKNOWN``; parse fallback now uses ``COMMAND``. ``SQLResult`` operation
  helpers now use canonical operation values directly, and
  ``create_sql_result()`` exposes explicit keyword arguments instead of
  accepting arbitrary ``**kwargs``.
* ``SQLFileLoader.get_sql()`` now compiles named statements on lookup and
  returns the cached ``SQL`` object for repeated normalized names until
  ``clear_cache()`` is called.
* Result and adapter internals dropped importable compatibility helpers:
  ``sqlspec.core.result._io`` and its ``rows_to_pandas()`` /
  ``rows_to_polars()`` helpers, ``ArrowOdbcTypeConverter``, ``BQ_TYPE_MAP``,
  ``DuckDBOutputConverter.convert_duckdb_value()``,
  ``DuckDBOutputConverter.prepare_duckdb_parameter()``, and
  ``psqlpy.normalize_scalar_parameter()``.
* Oracle cleanup removed ``OracleVectorType`` and the legacy
  ``OracleOutputConverter.detect_json_storage_type()``,
  ``OracleOutputConverter.format_datetime_for_oracle()``,
  ``OracleOutputConverter.handle_large_lob()``, and
  ``OracleOutputConverter.convert_oracle_value()`` helper methods.
* Migration internals moved. ``BaseMigrationRunner`` is no longer exported from
  ``sqlspec.migrations.base``; import it from
  ``sqlspec.migrations.runner`` if subclassing migration runners.
* PyMySQL no longer unwraps ``connection_config["extra"]`` into raw driver
  keyword arguments; pass driver kwargs directly in ``connection_config``.
* Several public implementation classes are now marked ``@final`` for
  typing/mypyc correctness. Downstream subclasses of these classes will fail
  static type checking. Affected classes include driver/converter internals
  such as ``AdbcDriver``, ``AdbcExceptionHandler``,
  ``BigQueryOutputConverter``, ``DuckDBOutputConverter``,
  ``SpannerOutputConverter``, builder wrapper/factory types,
  ``JoinBuilder``, ``SQLFactory``, ``OperationProfile``, ``CompiledSQL``,
  ``SQLProcessor``, dialect config classes, ``CachedQuery``, ``QueryCache``,
  event message/queue types, and ``MigrationVersion``.
* Performance cleanup tightened additional compatibility-sensitive contracts:
  storage ``backend_type`` is a class attribute, parameter builders expose
  ``generate_unique_parameter_name()``, statement observers are protocol based,
  and legacy aliases such as ``BackendNotRegisteredError`` were removed.

**Added:**

* Added ``Insert.values_from()``, ``Insert.values_from_many()``, and
  ``Update.set_from()`` for schema-aware SQL builders. These helpers accept
  dicts, dataclasses, msgspec structs, Pydantic models, and attrs classes while
  preserving Python attribute names for SQL columns.
* Added ``on_pool_destroying`` lifecycle hooks so components can release
  checked-out resources before pools close.
* Added runtime lifecycle hook registration through
  ``ObservabilityRuntime.register_lifecycle_hook()``.
* Added async lifecycle hook execution for pool, connection, session, query,
  and error events. Async SQLSpec paths now await hooks registered for
  ``on_pool_create``, ``on_pool_destroying``, ``on_pool_destroy``,
  ``on_connection_create``, ``on_connection_destroy``, ``on_session_start``,
  ``on_session_end``, ``on_query_start``, ``on_query_complete``, and
  ``on_error``.

**Fixed:**

* Reworked native event listener backends for ``asyncpg``, ``psycopg``,
  ``psqlpy``, and Oracle AQ to use persistent per-channel listeners, avoiding
  connection races, callback churn, dropped secondary subscriptions, and
  ignored Oracle ``poll_interval`` settings.
* Honored builder optimization flags by wiring explicit sqlglot optimizer
  rules, so ``optimize_joins``, ``optimize_predicates``, and
  ``simplify_expressions`` now disable only their matching steps instead of
  always running the full default pipeline.
* Passing a sqlglot ``Dialect`` class to EXPLAIN builders or
  ``StatementConfig.dialect`` now resolves to the correct dialect name.
* Avoided parser round-trips for simple builder identifiers and MERGE JSON
  source construction while preserving rendered SQL.
* Deferred temporal version-generator registration until temporal builder APIs
  are used. Code that hand-builds ``exp.Version`` nodes should call
  ``sqlspec.builder.register_version_generators()`` before rendering them.
* Routed async pool teardown through the base config lifecycle path so
  ``on_pool_destroy`` and ``on_pool_destroying`` fire consistently across async
  adapters.
* Registered binary ``json`` and ``jsonb`` codecs for AsyncPG and CockroachDB
  AsyncPG connections, allowing Arrow bulk loads into PostgreSQL JSON columns.
* Restored Litestar request decoding for handlers annotated with
  ``np.ndarray``.
* Bounded missing named-SQL error messages and added structured lookup context
  through ``SQLStatementNotFoundError``.
* Normalized framework ``orderBy`` aliases so camel-case API values can map to
  SQL-facing snake-case fields while preserving the configured field allowlist.
* Hardened BigQuery emulator handling for simple inserts and unsupported bulk
  load paths.
* Preserved Oracle implicit identifier casing for expression-backed query
  builder statements, fixing ``FOR UPDATE``, vector-distance, and migration
  tracker queries against unquoted Oracle objects.
* Preserved repeated same-named bind parameters in expression-backed pagination
  count and window-count queries.

**Performance:**

* Expanded mypyc coverage to sqlglot dialect helpers, data-dictionary dialects,
  selected extension helpers, ADK record types, and measured hot-path helpers.
* Added ``librt`` to the ``performance`` extra for compiled string assembly in
  SQL splitting and psqlpy copy encoding.

v0.46.3 - Plugin initialization and loader diagnostics
------------------------------------------------------------------------------

**Fixed:**

* ``SQLSpecPlugin.on_app_init()`` now mutates ``app_config.plugins`` in place,
  preserving Litestar plugin discovery for plugins registered later in the
  startup sequence.
* Missing named SQL statements now report bounded diagnostics instead of
  dumping every loaded statement name.

v0.46.2 - Framework filter wire-name normalization
------------------------------------------------------------------------------

**Fixed:**

* Framework filter providers now normalize configured sort fields against
  wire-facing names, fixing camel-case frontend values such as
  ``orderBy=uploadedCollections`` when the SQL field is snake_case.

v0.46.1 - Litestar filter provider binding
------------------------------------------------------------------------------

**Fixed:**

* Litestar generated filter providers now use unique dependency parameter names
  for sibling ``IN``, ``NOT IN``, null, not-null, and range filters, preventing
  values from one filter from binding to another.

v0.46.0 - Service typing and serializer registry
------------------------------------------------------------------------------

**Fixed:**

* Restored async and sync service overload narrowing for ``paginate()`` and
  ``get_one()`` when ``schema_type`` is provided.
* Extracted ``DEFAULT_TYPE_ENCODERS`` and applied them through the Litestar
  plugin while preserving user encoder precedence.
* Added Litestar decoders for NumPy arrays and ``uuid_utils.UUID`` values.

v0.45.0 - Services, filters, Oracle types, and Sanic
------------------------------------------------------------------------------

**Added:**

* Added first-party ``SQLSpecAsyncService`` and ``SQLSpecSyncService`` helpers
  with pagination, lookup, existence, and transaction convenience methods.
* Added Sanic framework integration.
* Added Oracle native JSON, VECTOR ergonomics, UUID/LOB handling, and smarter
  type coercion for Oracle workloads.

**Fixed:**

* Qualified statement filters correctly for joined queries and count queries.
* Tightened ``SearchFilter`` and ``NotInSearchFilter`` validation so unsupported
  field names fail instead of silently dropping predicates.
* Fixed raw ``ORDER BY`` handling and widened computed-column support for
  search and sort filters.
* Exposed LIKE-pattern escaping helpers for callers that bypass the standard
  filter pipeline.

v0.44.0 - Aiomysql, schema wire names, and pagination introspection
------------------------------------------------------------------------------

**Added:**

* Added the ``aiomysql`` adapter with driver, config, Arrow, migrations, ADK,
  event queue, Litestar store, data-dictionary, and integration coverage.

**Changed:**

* Removed the mock adapter and updated the testing docs around real adapter
  fixtures.
* Converted ``OffsetPagination`` to a stdlib dataclass while keeping the public
  import path intact.

**Fixed:**

* ``schema_dump()`` now honors msgspec ``rename=`` metadata for wire-format
  output.
* ``OffsetPagination`` preserves runtime annotations for mypyc wheels and
  Litestar OpenAPI generation.

v0.43.0 - SQLCommenter, ADK stale sessions, and docs build fixes
------------------------------------------------------------------------------

**Added:**

* Added Google SQLCommenter support.
* Added ADK stale-session detection.

**Fixed:**

* Added ParadeDB and pgvector dialect configuration to the SQL splitter.
* Fixed mypyc compilation issues, exception handling, filter providers, and
  vector-distance SQL generation.
* Removed the Sphinx Toolbox dependency to keep documentation building on
  Sphinx 9.x.

v0.42.0 - ADK store alignment
------------------------------------------------------------------------------

**Changed:**

* Overhauled the ADK backend to align with the ADK 1.0 store contract.

**Fixed:**

* Addressed serializer follow-ups found by mypyc builds.

v0.41.1 - Path and documentation fixes
------------------------------------------------------------------------------

**Fixed:**

* Resolved root paths to the parent directory for file-based paths.
* Fixed documentation references for vector distance and Flask examples.

v0.41.0 - Documentation, PostgreSQL dialects, and storage polish
------------------------------------------------------------------------------

**Added:**

* Added PostgreSQL extension dialect support.
* Added CSV format support for Arrow table export and import.

**Changed:**

* Overhauled the documentation structure and content.
* Moved sqlglot dialect modules into the top-level ``sqlspec.dialects``
  package.
* Improved mypyc configuration and CI validation paths.

**Fixed:**

* Supported set operations in pagination and count queries.
* Isolated AioSQLite in-memory databases with unique URIs per config instance.
* Added Oracle BLOB support and byte-length thresholds for LOB coercion.
* Used the portal fallback when ``await_()`` is called from an async task.
* Deduplicated named parameters and fixed ``SearchFilter`` placeholder reuse.

v0.40.0 - SQLGlot refresh
------------------------------------------------------------------------------

**Changed:**

* Updated the sqlglot dependency pin to the latest supported version.

v0.39.0 - Migration squash and hot-path performance
------------------------------------------------------------------------------

**Breaking changes:**

* Renamed storage sync methods to the ``*_sync`` pattern.
* Reworked the parsing pipeline around parse-once AST preservation and
  structural parameter fingerprinting.

**Added:**

* Added the migration squash engine.
* Added benchmark scripts and hot-path performance optimizations for parsing,
  parameter processing, serialization, and Arrow conversion.

**Fixed:**

* Fixed in-memory Arrow streaming with an async sentinel pattern.
* Improved AioSQLite pool shutdown and thread handling.
* Restored documentation search and hardened hot-path optimizations.

v0.38.4 - Pool and storage race fixes
------------------------------------------------------------------------------

**Fixed:**

* Fixed a race condition during connection pool initialization.
* Buffered storage streams consistently.

v0.38.3 - Connection lifecycle hooks and migration tracking
------------------------------------------------------------------------------

**Added:**

* Added ``on_connection_create`` lifecycle hooks.
* Improved migration logging and tracking.

**Fixed:**

* Fixed DuckDB variable persistence across connections.

v0.38.2 - Storage paths and logging options
------------------------------------------------------------------------------

**Added:**

* Added migration ``use_logger`` support and SQL logging
  ``include_driver_name`` controls.

**Fixed:**

* Fixed storage backend path handling.
* Avoided blocking behavior in async storage streaming.

v0.38.1 - Python 3.14 and compiled-wheel readiness
------------------------------------------------------------------------------

**Added:**

* Added Python 3.14 CI coverage and mypyc wheel builds.

**Fixed:**

* Fixed driver parameter normalization.
* Fixed Litestar plugin session-provider behavior.
* Fixed MySQL build issues.

v0.38.0 - Structured logging and exception mapping
------------------------------------------------------------------------------

**Added:**

* Added ``value_type`` support to ``select_value`` methods.
* Added structured SQL logging context and ``COMMAND`` operation logging.

**Changed:**

* Added more granular database exception mapping.

v0.37.1 - Column pruning and pagination filters
------------------------------------------------------------------------------

**Added:**

* Added column-pruning optimization.

**Fixed:**

* Fixed pagination parameter filtering.

v0.37.0 - Builder and count-query improvements
------------------------------------------------------------------------------

**Added:**

* Enhanced query-builder support for count queries.

v0.36.3 - Select helper corrections
------------------------------------------------------------------------------

**Fixed:**

* Corrected ``select_with_count`` and ``select_only`` behavior.

v0.36.2 - Exception handler edge cases
------------------------------------------------------------------------------

**Fixed:**

* Handled additional exception-handler edge cases.

v0.36.1 - DuckDB connection close behavior
------------------------------------------------------------------------------

**Fixed:**

* Closed DuckDB file-based connections on context-manager exit.

v0.36.0 - Documentation restructure and adapter exceptions
------------------------------------------------------------------------------

**Changed:**

* Restructured the documentation.

**Fixed:**

* Improved exception handling across adapters.

v0.35.0 - SQL class unification, ADK enhancements, and EXPLAIN
------------------------------------------------------------------------------

**Added:**

* Added dialect-aware ``EXPLAIN`` plan support.
* Added ADK enhancements and EXPLAIN-plan integration.
* Added type narrowing for parameter-conversion helpers.

**Changed:**

* Unified SQL class query modifications and expanded observability support.
* Simplified the event backend.
* Reorganized unit and integration tests.

v0.34.0 - Database event channels and utility IDs
------------------------------------------------------------------------------

**Added:**

* Added the database event channels extension with queue-backed publish/listen
  APIs and native backend support.
* Added UUID and ID generation utilities.

**Fixed:**

* Moved event configuration to the ``extension_config`` pattern.
* Fixed mypyc signature generation for portal helpers.

v0.33.0 - Config naming, multi-config resolution, and filter additions
------------------------------------------------------------------------------

**Breaking changes:**

* Standardized adapter config names from ``pool_config`` to
  ``connection_config`` and from ``pool_instance`` to ``connection_instance``
  across all adapters.

**Added:**

* Added environment-variable and ``pyproject.toml`` multi-config resolution for
  the CLI.
* Added ``NullFilter`` and ``NotNullFilter``.
* Added URL signing methods to storage object protocols and backends.
* Simplified ``add_config()`` return typing.

**Fixed:**

* Fixed AioSQLite 0.22 compatibility after ``Connection`` stopped inheriting
  from ``Thread``.
* Fixed builder edge cases, ``SearchFilter`` empty/``None`` handling, and
  ``Update.set()`` edge cases.

v0.32.0 - Spanner, vector search, and result conversion
------------------------------------------------------------------------------

**Added:**

* Added the Google Spanner driver.
* Added vector search support in the query builder.
* Added result conversion helpers for Arrow, Pandas, and Polars.
* Added driver ``fetch*`` compatibility aliases.

**Fixed:**

* Improved BigQuery ``execute_many`` bulk inserts.
* Improved Spanner write handling.
* Improved async handling for migration commands.

v0.31.0 - Data dictionary and execution correctness
------------------------------------------------------------------------------

**Added:**

* Added topological sorting and foreign-key retrieval enhancements.

**Fixed:**

* Correctly mapped ``execute_many`` parameters for all drivers.
* Fixed ``returns_row`` false negatives.
* Corrected query-builder edge cases and typing.
* Avoided DuckDB locks in testing documentation examples.

v0.30.2 - Compiled migration path fix
------------------------------------------------------------------------------

**Fixed:**

* Temporarily removed the migration path that was unsafe for compiled builds.

v0.30.1 - Mypyc and count-query fixes
------------------------------------------------------------------------------

**Fixed:**

* Fixed mypyc compatibility around dynamic imports and lifecycle dispatcher
  guard attributes.
* Validated ``FROM`` clauses during count-query generation.

v0.30.0 - Query stack, telemetry, and migration templates
------------------------------------------------------------------------------

**Added:**

* Added pipelined stack execution.
* Added telemetry integrations.
* Added DuckDB community-extension flags.
* Added improved migration template customization.

**Fixed:**

* Fixed Litestar sync context-manager handling.
* Corrected Oracle JSON support-version lookup.

v0.29.0 - Storage pipelines, connectors, and migration convenience
------------------------------------------------------------------------------

**Added:**

* Added sync and async storage capabilities and pipelines.
* Added Google Cloud SQL and AlloyDB connector support.
* Added Oracle RAW(16) UUID conversion and handlers.
* Added migration convenience methods to config classes.
* Added ``disable_di`` controls for framework integrations.

**Fixed:**

* Fixed migration crashes with null values and malformed regex patterns.
* Added Decimal JSON encoding support.
* Improved ``COPY`` detection, MERGE behavior, parameter profiles, and config
  consistency.

v0.28.1 - Empty SQL files and project commands
------------------------------------------------------------------------------

**Added:**

* Added SQLSpec project agent commands.

**Fixed:**

* Improved handling of empty SQL files.

v0.28.0 - Arrow support and additional framework extensions
------------------------------------------------------------------------------

**Added:**

* Added FastAPI, Starlette, and Flask extensions.
* Added the Arrow type-system foundation and ``select_to_arrow()`` support.
* Added native Arrow support for ADBC, DuckDB, BigQuery, PostgreSQL adapters,
  SQLite, MySQL, and Oracle.
* Added NumPy array serialization through the SQLSpec plugin.

**Fixed:**

* Updated ADK store signatures and session-key consistency.
* Made ADK store SQL table creation asynchronous.

v0.27.0 - ADK sessions, migrations, and Python 3.10 baseline
------------------------------------------------------------------------------

**Breaking changes:**

* Dropped Python 3.9 support and moved to Python 3.10+ type-hint syntax.
* Refactored the Litestar extension to remove wrapper classes and unify
  handlers.

**Added:**

* Added SQLSpec documentation, Litestar session backend support, and the Google
  ADK session backend.
* Added optional NumPy serialization and Oracle NumPy integration.
* Added ``schema_type`` support to ``SQLResult`` helper methods.
* Added hybrid timestamp/sequential migration versioning, transactional
  migrations, shell completion docs/tests, and migration author defaults from
  git config.

**Fixed:**

* Improved granular database exception handling and schema conversion caching.
* Fixed duplicate SQL file loading, migration dry-run handling, CLI path
  handling, and pgvector registration logging.
* Added automatic Oracle CLOB hydration for msgspec integration.

v0.26.0 - Data dictionary and async migrations
------------------------------------------------------------------------------

**Added:**

* Added data-dictionary support for database metadata.
* Added async migrations and callable config support.
* Added query-builder ``FOR UPDATE`` locking.
* Added ``bind_key`` support to all adapter configs.

**Changed:**

* Enhanced serialization, type conversion, sync tooling, and migration
  infrastructure.

v0.25.0 - Public API and NumPy decoder polish
------------------------------------------------------------------------------

**Added:**

* Added NumPy decoder support.

**Fixed:**

* Correctly handled duplicate use of the same bind parameter.
* Removed private-variable usage from public APIs.

v0.24.1 - RETURNING clause detection
------------------------------------------------------------------------------

**Fixed:**

* Correctly detected SQL ``RETURNING`` clauses.

v0.24.0 - Builder consolidation
------------------------------------------------------------------------------

**Added:**

* Added builder support for merged parameter names and ``OR`` composition.

**Changed:**

* Refactored builder code to reduce duplication.

Previous Versions
=================

For releases before ``v0.24.0``, see the repository tag history and GitHub
release records.
