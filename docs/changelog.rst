=========
Changelog
=========

All notable SQLSpec changes are summarized here. Entries are grouped by release
and focus on user-visible behavior, public API changes, compatibility notes, and
important operational fixes.

Recent Updates
==============

v0.61.1 - Compiled async exception handling
------------------------------------------------------------------------------

**Fixed:**

* Compiled async drivers no longer re-raise an exception already being handled
  by the caller when a database operation inside that handler succeeds.

v0.61.0 - Scoped memory recall and ADK modernization
------------------------------------------------------------------------------

**Added:**

* Google ADK memory stores now support scoped memory recall. The ``adk_memory``
  table stores a ``scope`` column (``'user'`` or ``'app'``) with composite
  indexes for efficient partitioned lookups across all 14 database dialect
  adapters.
* Memory recall retrieves both user-scoped memory (``scope = 'user'``) and
  app-scoped memory (``scope = 'app'``) by default, and supports filtering via
  ``scope_filter`` (``"all"``, ``"user"``, or ``"app"``).
* Memory ingestion methods (``add_memories``, ``add_events_to_memory``, and
  ``add_session_to_memory``) accept a ``scope`` argument (defaulting to
  ``'user'``).
* New :mod:`sqlspec.extensions.adk.maintenance` module provides
  :func:`~sqlspec.extensions.adk.maintenance.prune_events`,
  :func:`~sqlspec.extensions.adk.maintenance.prune_sessions`, and
  :func:`~sqlspec.extensions.adk.maintenance.prune_user_state` for programmatic
  retention maintenance with structured deletion reporting.
* The ``adk_event`` table now persists ``app_name`` and ``user_id`` directly
  across all database adapters, backed by ``(app_name, timestamp)`` composite
  indexes.
* psycopg now supports AlloyDB / PostgreSQL 17+ BM25 full-text indexing, vector
  embeddings, and hybrid similarity ranking parity with asyncpg.
* :meth:`MemoryService.search_memory() <sqlspec.extensions.adk.memory.service.MemoryService.search_memory>`
  accepts an optional ``embedding`` vector parameter to fuse vector similarity
  scoring with text search.
* Added ADK configuration options for vector search: ``vector_index_type``,
  ``vector_dimensions``, ``enable_bm25``, ``scann_num_leaves``, and
  ``scann_quantizer``.

**Changed:**

* ADK domain record types are modernized to clean domain models:
  ``StoredMemory``, ``StoredSession``, ``StoredEvent``, and ``StoredArtifact``.
  Legacy type aliases (``MemoryRecord``, ``SessionRecord``, etc.) have been
  removed.
* ADK database table names are standardized to singular form across all 14
  dialect adapters: ``adk_session``, ``adk_event``, ``adk_memory``,
  ``adk_app_state``, ``adk_user_state``, ``adk_artifact``, and
  ``adk_internal_metadata``.
* ADK table migration ``0001_create_adk_tables.py`` consolidates singular table
  names and the ``scope`` column into the base migration.
* Table maintenance (``maintain_tables``) strictly performs row retention
  pruning across all stores; dialect-specific storage commands (``VACUUM``,
  ``ANALYZE``, ``CHECKPOINT``, etc.) and the unused ``reindex`` flag have been
  removed.
* Oracle ADK storage features (compression, in-memory, partitioning) emit DDL
  clauses directly without dynamic ``v$option`` server probes at runtime.

**Fixed:**

* ADK retention pruning (``delete_entries_older_than``) now consistently honors
  ``app_name`` filtering across all stores and database adapters.
* Arrow conversion now preserves declared SQL / cursor column types for columns
  containing only ``NULL`` values when using psycopg or MySQL drivers
  (aiomysql, asyncmy, mysqlconnector, pymysql), preventing type collapse to
  ``null`` or ``string``.

v0.60.0 - Queue limits
------------------------------------------------------------------------------

**Added:**

* The new ``listener_queue_capacity`` setting caps each PostgreSQL listener
  queue. It works with asyncpg, psqlpy, and sync or async psycopg listeners.
  The default has no limit. Bad values raise
  :class:`~sqlspec.exceptions.ImproperConfigurationError`.
* :class:`~sqlspec.extensions.litestar.channels.SQLSpecChannelsBackend` accepts
  ``output_queue_capacity``. It caps decoded messages from any async event
  transport, including PostgreSQL, Oracle, and polling channels. The default
  has no limit. Bad values raise ``ValueError``.
* ``output_queue_depth`` shows the current Litestar backlog.
  ``dropped_message_count`` shows the total number of overflow drops.
* PostgreSQL listener metrics now track ``events.listener.queue.depth`` and the
  total ``events.listener.queue.dropped`` count for the hub.

**Changed:**

* A full capped queue drops its oldest item before it adds the new one. Each
  PostgreSQL consumer has its own queue. Shutdown clears queued items and sets
  depth to zero. Drop counts stay in place after a restart.
* An overflow in PostgreSQL ``notify`` drops transient data. With
  ``notify_queue``, it drops only a wake-up marker. The durable row stays in the
  table and can be found by the next scan.
* Oracle AQ and TxEventQ still read from their native queues. They do not add a
  listener queue. Table-backed ``poll_queue`` stores check
  ``listener_queue_capacity``, but the setting does not change how they poll.
* Bad Litestar channel payloads are logged and acknowledged. They do not increase
  ``dropped_message_count``.

v0.59.0 - Data dictionary and loader access
------------------------------------------------------------------------------

**Added:**

* :attr:`SQLSpec.loader <sqlspec.base.SQLSpec.loader>` gives read-only access to
  the registry's SQL file loader. SQLSpec creates the loader on first access
  when one was not supplied.
* Object storage backends expose ``resolve_uri(path)`` to format one
  backend-relative path as an absolute local path or protocol-qualified remote
  URI without storage I/O. Custom backends that implement
  ``ObjectStoreProtocol`` must define it too.

**Changed:**

* Explicit database cancellation now raises
  :class:`~sqlspec.exceptions.OperationCancelledError`. Timeouts and deadlines
  still raise :class:`~sqlspec.exceptions.QueryTimeoutError`. Both exceptions
  inherit from :class:`~sqlspec.exceptions.OperationalError`. Applications that
  caught ``QueryTimeoutError`` for both outcomes must catch both exceptions, or
  catch ``OperationalError``.
* Data dictionary SQL files now live beside each dialect package. SQLSpec loads
  these files through package resources.
* Data dictionary query helpers now use separate ``domain`` and ``operation``
  names. ``mode`` is optional. Update direct calls that pass one flat query
  name.
* Spanner now sends UUID values as 36-character text, not base64 bytes. To keep
  UUID objects unchanged, set
  ``driver_features={"enable_uuid_conversion": False}``. Text is the new
  default.

**Fixed:**

* SQL files supplied with Windows drive paths now resolve from their requested
  directory instead of the process working directory.
* Async statement errors from mypyc-compiled drivers are translated into
  :class:`~sqlspec.exceptions.SQLSpecError` instead of terminating the process.
* ADBC adapters for PostgreSQL now keep ``None`` in arrays. Each value binds as
  SQL ``NULL``. This keeps null values in place.

v0.58.3 - Data and store fixes
------------------------------------------------------------------------------

**Fixed:**

* Nested msgspec structs now use their own encoded field names. This works for
  optional fields, annotations, lists, tuples, and maps. Mixed rename rules no
  longer reuse the outer struct's rule. Keys in plain maps stay unchanged.
* ``ensure_async_()`` now has a
  :class:`collections.abc.Coroutine` return type. This matches the value it has
  always returned at run time and removes the need for a cast.
* SQLSpec Litestar session stores now inherit
  :class:`litestar.stores.base.Store`. They keep its async context manager and
  work with ``StoreRegistry``.
* Each extension now loads its SQL migration query names on its own. A file
  such as ``0001_create_table.sql`` can use ``migrate-0001-up`` and
  ``migrate-0001-down`` with no clash across extensions. SQLSpec still stores
  the prefixed tracker version.

v0.58.2 - SQL file parameter diagnostics
------------------------------------------------------------------------------

**Fixed:**

* Malformed ``-- param:`` directives now produce actionable warnings that name
  the SQL file, line number, and malformed directive. Structured log records
  expose the numeric ``line_number`` and textual ``directive`` separately.
  Non-strict loading continues to warn and skip malformed directives, while
  strict loading continues to raise
  :class:`~sqlspec.exceptions.SQLFileParseError`.

v0.58.1 - Migration template configuration
------------------------------------------------------------------------------

**Fixed:**

* ``migration_config`` accepts the three keys that customize generated
  migration files: ``templates``, ``default_format``, and ``title``. The
  key validation added in v0.58.0 did not recognize them, so any configuration
  using a customized migration template raised
  :class:`~sqlspec.exceptions.ImproperConfigurationError` at construction.
  ``default_format`` was additionally reported with a suggestion to use
  ``default_schema``, an unrelated setting.
* Template overrides are validated when the configuration is built rather than
  when a migration is generated. A misspelling inside ``templates.sql`` or
  ``templates.py`` reports its full path and the closest valid key, and an
  override that is not a mapping is named along with the type supplied.
* :class:`~sqlspec.config.MigrationConfig` documents the real default for
  ``version_table_name``, which is ``ddl_migrations``.

**Added:**

* :class:`~sqlspec.config.MigrationTemplates`,
  :class:`~sqlspec.config.SQLTemplateOverride`, and
  :class:`~sqlspec.config.PythonTemplateOverride` describe the template
  override shape, so type checkers now cover it.
* The migrations guide documents template customization, including the
  placeholders available to each fragment.

v0.58.0 - Configuration and storage correctness
------------------------------------------------------------------------------

**Changed:**

* ``migration_config`` now rejects keys SQLSpec does not read, raising
  :class:`~sqlspec.exceptions.ImproperConfigurationError` with the closest
  valid key. A misspelling such as ``version_table`` instead of
  ``version_table_name`` was previously accepted and silently ignored, leaving
  the setting at its default. Remove or correct unrecognized keys to upgrade.
* Storage writes reject formats that cannot carry the payload being written,
  raising :class:`~sqlspec.exceptions.StorageCapabilityError` before any
  encoding or storage I/O. Row writes accept only ``json`` and ``jsonl``; Arrow
  table writes accept only ``parquet``, ``arrow-ipc``, and ``csv``. A mismatched
  format previously wrote one payload type under another format label. Read APIs
  still accept all five formats.
* ``stream_arrow_sync()`` and ``stream_arrow_async()`` accept only
  ``file_format="parquet"`` and raise
  :class:`~sqlspec.exceptions.StorageCapabilityError` for other formats. Use the
  regular Arrow read APIs for CSV, Arrow IPC, JSON, and JSONL payloads.
* JSONL payloads decode through PyArrow's native JSON reader. Its type inference
  applies to the result, so date-like strings now decode as Arrow timestamps
  rather than strings.

**Fixed:**

* Arrow batch streaming reads one Parquet row group at a time across the local,
  fsspec, and obstore backends, and accepts a ``batch_size`` bounding each record
  batch. The obstore backend streams through its seekable reader instead of
  buffering the whole object in memory, resolves cloud ``base_path`` only once,
  and closes readers deterministically when a stream is closed early.
* Decoding a JSONL payload containing a row larger than 1 MiB no longer fails
  with ``ArrowInvalid: straddling object straddles two block boundaries``.
* ADBC PostgreSQL connections recover after a failed statement. The aborted
  transaction is now cleared through the connection rather than by sending a
  ``ROLLBACK`` statement on a cursor, which the driver rejects on a connection
  that is already in an error state. Every later statement on that connection
  previously failed with ``INVALID_STATE: [libpq] cannot start transaction``.
  Uncommitted work in the aborted transaction is discarded, as PostgreSQL
  requires; commit before a statement whose failure you intend to recover from.
* Pointing ``--config`` at a module rather than a configuration object now
  reports the ``module:attribute`` references that module exports, instead of
  failing later with ``AttributeError: module has no attribute 'bind_key'``.
* Configuration references that misuse ``:`` report the accepted syntax rather
  than an import failure.
* Errors raised while importing a configuration module keep their original type
  and message instead of being rewrapped as an import failure.
* The "No SQLSpec config found" help text shows the ``[tool.sqlspec]`` section
  name, which console markup previously consumed, and no longer mangles config
  paths that contain colons.
* ``author`` is declared on :class:`~sqlspec.config.MigrationConfig`. The
  migration generator already read it, but type checkers rejected it.
* psycopg record loads preserve JSON and JSONB object shapes through Arrow COPY.
  JSON mappings previously reached psycopg COPY as Python dictionaries, which it
  cannot adapt in text COPY mode.

**Performance:**

* ``AsyncpgDriver.load_from_records()`` writes records directly with one binary
  COPY call instead of round-tripping them through Arrow. The removed conversion
  dominated small and medium batches; large batches also overtake
  ``executemany()`` throughput.

v0.57.0
------------------------------------------------------------------------------

**Added:**

* Packages distributed separately from SQLSpec can now ship Python migrations.
  Set ``migrations_path`` in an ``extension_config`` entry to point at a
  directory or a ``'<dotted.module>:<subdir>'`` specification. The extension is
  discovered and auto-included without appearing in ``include_extensions``.
* Added ``add_extension_migrations(name, migrations_path, settings=None)`` on
  database configurations, for packages that register migrations at runtime
  rather than declaratively.

**Fixed:**

* Extension names containing underscores no longer lose their version. A
  migration such as ``ext_my_extension_0001_init.sql`` previously resolved to the
  version ``ext_my_extension``, dropping ``0001`` and recording a malformed
  version in the migration tracking table.
* A configured extension that cannot be resolved now reports which module was
  tried and that no migrations were registered, instead of the ambiguous
  ``Extension <name> not found``.
* Extensions that ship no migrations directory no longer log a warning. Six of
  the bundled extensions have no migrations by design, so the warning was noise.
* :func:`sqlspec.utils.module_loader.module_to_os_path` resolves namespace
  packages to their search location instead of returning a path named ``None``.
* Compiled wheels now return correct results from :func:`isinstance` and
  :func:`issubclass` across SQLSpec class hierarchies. A previous check could
  poison a shared abstract-base cache and cause later query-builder execution
  to fail.
* Compiled migration runners no longer raise :class:`TypeError` while resolving
  the default schema when no configuration is attached.

**Changed:**

* :func:`sqlspec.utils.module_loader.module_to_os_path` raises
  :class:`ModuleNotFoundError` rather than :class:`TypeError` when a module
  cannot be found, so callers can catch the real condition.
* SQLSpec base classes no longer use ``ABCMeta`` at runtime because mypyc shares
  its abstract-base caches across compiled class hierarchies. Static type
  checkers still enforce abstract methods. Runtime code should not rely on
  ``inspect.isabstract()`` or abstract-class instantiation errors for these
  bases. ``StatementResult`` remains structurally iterable.

**Known limitations:**

* Extension-owned SQL migration files are discovered but cannot yet resolve
  their prefixed named queries. Separately distributed packages should ship
  Python migration files for this release.

v0.56.2
------------------------------------------------------------------------------

**Added:**

* Added ``uuid_from_string()``, ``uuid_from_bytes()``, and ``uuid_from_int()``
  in :mod:`sqlspec.utils.uuids`. They always return :class:`uuid.UUID`. Text
  parsing uses Rust when ``uuid-utils`` is installed.

**Fixed:**

* PostgreSQL-family ADBC row APIs now decode scalar ``UUID`` and ``UUID[]`` data
  to Python UUIDs. This works for buffered and streamed rows. Native Arrow
  results keep the extension schema, and
  ``enable_arrow_extension_types=False`` restores raw storage bytes on row
  APIs.
* Oracle 12c through 20c can bind direct Python JSON values to ``BLOB IS JSON``
  columns. SQLSpec writes UTF-8 JSON to a BLOB locator for sync, async, batch,
  and streaming calls. Oracle 21c and newer still use native ``JSON`` binding.
  Explicit ``OracleClob`` values remain CLOBs.

**Changed:**

* BigQuery ``load_from_records()`` now reuses fully checked lists of plain
  dictionaries when no fields must move. It still copies rows for explicit
  columns, mapping subclasses, and different key orders.
* Spanner and Oracle now reuse bind data when no value needs a conversion. They
  copy only after the first changed value. Bound values and checks are
  unchanged.
* UUID parsing now uses :mod:`sqlspec.utils.uuids` across adapters and type
  converters.

**Docs:**

* The Oracle guide now covers JSON storage, LOB, UUID, and VECTOR behavior. It
  also covers driver options and an Oracle ``MERGE`` upsert recipe.
* The ADBC guide now explains UUID row and Arrow results. It also documents the
  ``enable_arrow_extension_types`` switch.

v0.56.1
------------------------------------------------------------------------------

**Added:**

* PostgreSQL-family ADBC drivers accept a list or tuple of UUIDs as a single
  array parameter, so queries such as
  ``WHERE id = ANY(CAST(? AS UUID[]))`` now work. The cast is added
  automatically when the query does not already supply one.

**Fixed:**

* PostgreSQL-family ADBC drivers no longer fail when a UUID is a statement's
  only parameter. Repeated executions of such a statement previously reused a
  cached plan that skipped UUID binding and reached PostgreSQL as ``bytea``.
* ``EXPLAIN`` statements now return their query plan. Explained statements were
  classified as non-row-returning, so drivers ran the ``EXPLAIN`` and then
  discarded the plan, leaving ``select()`` and ``execute()`` with no rows. This
  affected every adapter except DuckDB, ADBC, and BigQuery. ``SHOW`` and
  ``DESCRIBE`` are now classified the same way. Oracle ``.explain()`` calls now
  tag the plan-table entry, return ``DBMS_XPLAN`` rows, and remove the entry
  before returning. Raw caller-owned ``EXPLAIN PLAN`` statements remain
  unchanged. (`#655 <https://github.com/litestar-org/sqlspec/issues/655>`_)
* Explaining a statement no longer discards its configuration. An explained
  PostgreSQL statement previously compiled with ``?`` placeholders instead of
  ``$n``, and a named parameter used more than once was sent once per use
  instead of being deduplicated. ``SQL.copy(statement_config=...)`` also raised
  ``TypeError`` and now accepts an override.
* ``TABLE table_name`` statements now return their rows on PostgreSQL, DuckDB,
  and MySQL. SQLGlot does not currently model this shorthand for
  ``SELECT * FROM table_name``, so SQLSpec previously classified it as a
  non-row-returning command and discarded the result.

**Changed:**

* PostgreSQL-family ADBC drivers convert UUID parameters faster. UUID objects
  are now formatted directly instead of being re-parsed on every execution,
  which roughly halves the conversion cost of large ``execute_many`` batches.
  Bound values are unchanged.
* Binding a UUID array containing ``None`` now raises an explicit error. The
  PostgreSQL ADBC driver encodes null array elements as empty strings, which
  PostgreSQL rejects for ``UUID[]``; the previous behavior was an opaque
  ``invalid input syntax for type uuid`` failure from the server.

v0.56.0
------------------------------------------------------------------------------

**Breaking changes:**

* Extension storage keys that a selected ADK, Litestar, or Events backend
  cannot honor now raise an explicit configuration error instead of being
  silently ignored.

**Added:**

* New ``SchemaTarget`` and ``SchemaEnsureResult`` types plus sync and async
  schema checks can create missing tables and add columns. Use
  ``ensure_schema_sync()`` or ``ensure_schema_async()`` for each driver mode.
  ADK, Litestar session, and durable event stores expose ``manage_schema``,
  ``create_schema``, and ``run_migrations`` controls for this lifecycle.
* Oracle ADK, durable event, and Litestar session tables now share opt-in
  compression, partitioning, In-Memory, and table-option configuration.
* BigQuery session and queue partition options and CockroachDB session hash
  sharding and row-level TTL are now available.
* Applicable Litestar, Events, and ADK stores now expose PostgreSQL table and
  autovacuum tuning, MySQL and MariaDB table/index options, Spanner sharding and
  table/index options, and opt-in SQLite extension PRAGMA profiles.

**Changed:**

* Raised the minimum supported ``sqlglot`` and ``sqlglot[c]`` version to
  30.13.0.
* ADK, Litestar session, and durable event stores now derive additive schema
  currency from their canonical DDL. ADK no longer seeds or bumps a
  ``schema_version`` row for additive changes.
* Oracle server-version, JSON storage, and extension-table capability detection
  now share the config/pool-scoped data dictionary cache. Requested storage
  optimizations degrade to structured warnings when an option is unavailable.

**Fixed:**

* PostgreSQL-family ADBC connections now bind top-level UUID parameters as
  PostgreSQL ``uuid`` values across ordinary, batch, streaming, and Arrow
  execution routes. (`#650 <https://github.com/litestar-org/sqlspec/issues/650>`_)
* Psycopg sync and async transactions now restore the connection's original
  autocommit mode after SQLSpec-owned commit or rollback operations (`#648`_).
* Spanner data-dictionary queries now cast nullable metadata filters to their
  concrete ``STRING`` or ``TIMESTAMP`` types, avoiding conflicting parameter
  inference when optional filters are omitted.
* Builder caching now reuses value-independent expression templates and binds
  each call's current parameters and statement configuration. This also
  isolates CTE bodies and returned ASTs instead of sharing mutable cached
  objects. (`#644 <https://github.com/litestar-org/sqlspec/issues/644>`_)
* Optimized-expression cache keys now include complete schema table, column,
  and type information, preventing different same-sized schemas from sharing
  an incompatible optimized AST.
* ``mssql_python`` transactions now use the driver's DBAPI transaction state,
  persist committed work, roll back pending work, and restore the connection's
  original autocommit mode. (`#642 <https://github.com/litestar-org/sqlspec/issues/642>`_)
* ``psqlpy`` now reports exact affected-row counts for non-returning single-row
  and multi-row ``INSERT``, ``UPDATE``, and ``DELETE`` statements while
  preserving the existing ``RETURNING`` result path.
  (`#645 <https://github.com/litestar-org/sqlspec/issues/645>`_)
* ``mssql_python`` now materializes cached result rows as real tuples, matching
  its declared result format. (`#630 <https://github.com/litestar-org/sqlspec/issues/630>`_)
* ``mssql_python`` and ``pymssql`` now classify SQL Server constraint messages
  even when the native driver omits or embeds the numeric error code, and
  ``pymssql`` translates named pyformat input to its reliable positional
  execution style.

v0.55.0
------------------------------------------------------------------------------

**Breaking changes:**

* SQLite and aiosqlite connections now follow the stdlib ``sqlite3`` default by
  leaving ``PRAGMA foreign_keys`` disabled unless
  ``connection_config={"enable_foreign_keys": True}`` is passed. Both adapters
  now share a 5000 ms busy timeout and aligned optimization PRAGMAs when the
  default ``enable_optimizations=True`` setting is active.
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

.. _#648: https://github.com/litestar-org/sqlspec/issues/648

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
  write, returning the updated ``StoredSession``.

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
  return the updated ``StoredSession``.
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
