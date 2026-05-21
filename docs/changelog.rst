=========
Changelog
=========

All notable SQLSpec changes are summarized here. Entries are grouped by release
and focus on user-visible behavior, public API changes, compatibility notes, and
important operational fixes.

Recent Updates
==============

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
* Performance cleanup tightened several compatibility-sensitive contracts:
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

**Deprecated:**

* Deprecated ``Insert.values_from_dict()`` and ``Insert.values_from_dicts()`` in
  favor of ``values_from()`` and ``values_from_many()``. Removal is planned for
  ``0.48.0``.

**Fixed:**

* Reworked native event listener backends for ``asyncpg``, ``psycopg``,
  ``psqlpy``, and Oracle AQ to use persistent per-channel listeners, avoiding
  connection races, callback churn, dropped secondary subscriptions, and
  ignored Oracle ``poll_interval`` settings.
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
