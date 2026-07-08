===============
Data Dictionary
===============

The data dictionary is SQLSpec's database-introspection interface. Use it to ask
a driver what metadata it can provide, then fetch schemas, objects, table
details, constraints, indexes, DDL, dependencies, and opt-in system metadata
through structured result objects.

Metadata Contract
=================

The data dictionary uses these public shapes:

- ``MetadataCapabilityProfile`` reports support by domain and adapter.
- ``MetadataCapability`` distinguishes ``supported``, ``unsupported``,
  ``unknown``, and ``not_implemented`` domains.
- ``MetadataResult`` wraps domain lookups, including unsupported domains.
- ``ObjectIdentity`` carries catalog, schema, object name, object type, dialect,
  quoted name, and metadata source.
- ``DDLResult`` carries DDL text plus ``native``, ``generated``, ``hybrid``,
  ``lossy``, or ``unsupported`` fidelity.
- ``SystemMetadataCapability``, ``SystemMetadataRequest``, and
  ``SystemMetadataResult`` keep operational metadata separate from structural
  metadata.
- Dependency graph helpers model typed edges such as foreign keys, views,
  sequence use, triggers, routines, partitions, grants, and extension-owned
  objects.
- System and performance metadata stays separate from structural metadata and
  is exposed only by adapters that report support for those domains.

For ``MetadataResult`` and ``SystemMetadataResult`` responses, inspect
``result.capability`` before using ``result.items`` or ``result.rows``. For
``DDLResult`` responses, inspect ``result.status`` and ``result.fidelity``.
That makes unsupported metadata distinct from an empty result set.

Capability Vocabulary
=====================

Support values:

- ``supported``: the adapter implements the domain.
- ``gated``: the domain exists but requires an explicit opt-in flag before it
  can run.
- ``unsupported``: the database or adapter cannot provide the domain.
- ``unknown``: no capability has been declared for the domain.
- ``not_implemented``: SQLSpec has a contract but no implementation yet.

Fidelity values:

- ``native``: returned by a database catalog, native API, or exact stored
  definition.
- ``generated``: reconstructed by SQLSpec from catalog metadata.
- ``hybrid``: combines native fragments and generated structure.
- ``lossy``: useful but incomplete; replay or export output may need review.
- ``partial``: only part of the domain is available.
- ``transport_fallback``: provided by a driver transport API such as ADBC.
- ``unsupported``: no reliable metadata is available.

Risk gates describe why metadata may be hidden, expensive, or unavailable:
``privileged``, ``billed``, ``expensive``, ``license_gated``, ``redacted``,
``managed_service_limited``, ``extension_required``, and ``version_gated``.

Capability-First Usage
======================

Ask for capabilities before assuming a domain exists:

.. code-block:: python

   profile = db.data_dictionary.get_metadata_capabilities(db)
   table_capability = profile.get("tables")

   if table_capability.support == "supported":
       result = db.data_dictionary.get_table_details(db, "orders", schema="public")
       for item in result.items:
           ...

Unsupported metadata is explicit:

.. code-block:: python

   result = db.data_dictionary.get_privileges(db, "orders", schema="public")

   if result.capability.support == "unsupported":
       # The adapter cannot answer this domain. This is not an empty grant list.
       ...
   elif not result.items:
       # The domain is supported, but no privileges matched.
       ...

Support Matrix
==============

The exact answer for a driver is always its runtime capability profile. This
matrix summarizes the implemented support families and the expected fidelity
terms callers should branch on.

.. list-table:: Metadata domain support by family
   :header-rows: 1
   :widths: 18 20 18 18 18 18

   * - Family
     - Core objects
     - Constraints/indexes
     - DDL
     - Dependencies
     - System/performance
   * - PostgreSQL
     - Native ``pg_catalog`` structural metadata.
     - Native constraints and indexes with catalog definitions.
     - Native or hybrid, depending on object type.
     - Native catalog edges.
     - Gated by default; settings and ``pg_stat`` domains require explicit
       opt-in.
   * - CockroachDB
     - Stable ``information_schema`` and compatible catalog metadata.
     - Stable-core support; internal metadata is opt-in.
     - Lossy or generated where exact DDL is unavailable.
     - Stable metadata only by default.
     - ``crdb_internal`` domains are disabled unless explicitly requested.
   * - MySQL and MariaDB
     - Native ``information_schema`` metadata with separate MariaDB handling.
     - Native constraints and indexes where exposed by the server.
     - Native ``SHOW CREATE`` output with session context.
     - Partial; sequence and event behavior differs by engine.
     - Performance Schema, ``sys``, plugins, and status domains are opt-in.
   * - Oracle
     - Native dictionary views.
     - Native constraints and indexes.
     - Native ``DBMS_METADATA`` where privileges allow it.
     - Native dictionary edges where available.
     - Diagnostics views are gated and may require license acknowledgement.
   * - SQL Server
     - Native catalog and ``INFORMATION_SCHEMA`` metadata.
     - Native constraints and indexes.
     - Generated or hybrid DDL with explicit fidelity.
     - Native catalog edges where available.
     - DMVs and Query Store are gated and redacted by default.
   * - SQLite
     - Native schema tables and PRAGMAs.
     - Native indexes and foreign keys; checks can be partial.
     - Native schema SQL when stored; parsed dependencies may be lossy.
     - Parsed/native edges for tables, views, and triggers.
     - Safe PRAGMAs are opt-in; integrity checks are explicit.
   * - DuckDB
     - Native ``duckdb_*`` catalog functions.
     - Native constraints and indexes where exposed.
     - Native or generated from catalog SQL.
     - Native ``duckdb_dependencies()`` where available.
     - Settings, memory, logs, and extensions are opt-in.
   * - BigQuery
     - ``INFORMATION_SCHEMA`` by project, dataset, and region.
     - Partial; constraints depend on BigQuery metadata availability.
     - Native table DDL where ``INFORMATION_SCHEMA.TABLES.ddl`` is available.
     - Partial and dataset-scoped.
     - Jobs, reservations, timelines, and sessions are risk-disclosed; check
       domain-level system capabilities before execution.
   * - Spanner
     - GoogleSQL and PostgreSQL dialect query packs.
     - Dialect-specific catalog support.
     - Admin API or catalog DDL depending on driver path.
     - Partial.
     - ``SPANNER_SYS`` and optimizer stats are operational metadata; check
       domain-level system capabilities before execution.
   * - ADBC
     - Transport fallback through driver metadata APIs when available.
     - Lossy constraints from ``GetObjects``; indexes use dialect packs.
     - Unsupported as a lossless source.
     - Not portable as a complete dependency source.
     - Table statistics are opt-in transport metadata.
   * - arrow-odbc
     - Dialect SQL packs and Arrow schema probes.
     - Dialect SQL packs where available.
     - Unsupported as a lossless source.
     - Unsupported until a raw ODBC catalog bridge exists.
     - Raw ODBC catalog APIs are explicitly unavailable in Python today.

Result Envelopes And DDL
========================

Some convenience methods return plain lists:

.. code-block:: python

   tables = db.data_dictionary.get_tables(db, schema="public")
   for table in tables:
       ...

Table, column, index, and foreign-key convenience calls can use that shape.
Domain lookups return result envelopes:

.. code-block:: python

   result = db.data_dictionary.get_objects(db, schema="public")

   if result.capability.support == "supported":
       for object_metadata in result.items:
           ...
   else:
       log.warning("object metadata unavailable: %s", result.warnings)

Use DDL fidelity instead of assuming a returned string is replayable. Object DDL
lookups return a ``DDLResult`` directly:

.. code-block:: python

   ddl = db.data_dictionary.get_ddl(db, "orders", schema="public")

   if ddl.status == "supported" and ddl.fidelity in {"native", "hybrid"}:
       apply_to_review_buffer(ddl.ddl)
   elif ddl.fidelity == "lossy":
       request_manual_review(ddl)
   else:
       skip_export(ddl.warnings)

Use ``get_schema_ddl()`` when you need a collection of DDL items. That method
returns a ``MetadataResult`` whose items are ``DDLResult`` instances.

Sort DDL by typed dependencies before replay. Dependency-capable adapters should
return enough edge metadata for callers to order objects before exporting or
replaying them:

.. code-block:: python

   dependencies = db.data_dictionary.get_dependencies(db, schema="public")
   ddl = db.data_dictionary.get_ddl(db, "orders", schema="public")

   if dependencies.capability.support == "supported":
       ordered_items = order_for_replay((ddl,), dependencies.items)

System And Performance Metadata
===============================

System metadata is disabled by default because it can expose SQL text, users,
hosts, settings, grants, connection strings, operational topology, billing data,
or license-gated diagnostics.

Use system metadata capability disclosures and explicit request flags before
requesting these domains:

.. code-block:: python

   from sqlspec.data_dictionary import SystemMetadataRequest

   profile = db.data_dictionary.get_metadata_capabilities(db)

   if profile.get("system").support == "supported":
       request = SystemMetadataRequest(
           "table_statistics",
           include_performance=True,
           schema="public",
           table="orders",
       )
       result = db.data_dictionary.get_system_metadata(db, request)

       if result.capability.support == "supported":
           for row in result.rows:
               ...

When an adapter exposes domain-level disclosures through
``get_system_metadata_capabilities()``, use that method to inspect the exact
system metadata domain before constructing a request. The returned
``SystemMetadataResult.capability`` is still the final execution status for the
specific request.

Rows should be redacted by default. Include sensitive values only for trusted
diagnostics workflows.

Cloud Notes
===========

BigQuery metadata must be scoped carefully. ``INFORMATION_SCHEMA`` views are
qualified by dataset, project, or region. Job, timeline, reservation, and
session-style metadata can be region-bound or billed. Structural capability
profiles disclose those risks; direct system metadata calls should still branch
on ``get_system_metadata_capabilities()`` before calling
``get_system_metadata()``.

Spanner has separate GoogleSQL and PostgreSQL dialect metadata shapes. Select the
driver dialect mode explicitly and treat ``SPANNER_SYS`` statistics as
performance metadata, not structural metadata. As with BigQuery, branch on the
system metadata capability API before attempting operational statistics queries.

Transport Fallback Notes
========================

ADBC and ODBC are transport layers, not database catalogs. They can expose
portable discovery metadata, but dialect query packs remain the source for
DDL-grade output. Treat ``transport_fallback`` and ``lossy`` fidelity as
inspection metadata unless the target workflow explicitly accepts those limits.

SQLSpec's bundled dialect query packs are maintained SQLSpec query packs based
on public catalog behavior.
