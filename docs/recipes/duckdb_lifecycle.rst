======================================
DuckDB Lifecycle for Isolated Databases
======================================

Use one ``DuckDBConfig`` per isolated database file and close the pool when the
run that owns that file finishes. This pattern fits batch jobs, temporary
analytics files, and other workflows where each run owns a separate DuckDB
database and may use DuckDB extensions.

Extension Setup
===============

Core DuckDB extensions, such as ``postgres``, can be configured by name only.
SQLSpec loads them for each physical connection and relies on DuckDB's autoload
support:

.. code-block:: python

   driver_features = {
       "extensions": [
           {"name": "postgres"},
       ],
   }

Community extensions, such as ``encodings``, are not core autoload extensions.
Bundle them into a shared ``extension_directory`` at image-build time when you
need network-free startup. If runtime installation is acceptable, request an
explicit install and allow community extensions:

.. code-block:: python

   from pathlib import Path

   from sqlspec.adapters.duckdb import DuckDBConfig


   def build_isolated_config(db_path: Path, extension_dir: Path) -> DuckDBConfig:
       return DuckDBConfig(
           connection_config={
               "database": str(db_path),
               "extension_directory": str(extension_dir),
               "allow_community_extensions": True,
           },
           driver_features={
               "extensions": [
                   {"name": "postgres"},
                   {"name": "encodings", "install": True, "required": True},
               ],
           },
       )

For fully offline deployments, install ``encodings`` into the shared
``extension_directory`` before runtime and keep the same config. DuckDB will use
the local extension cache instead of downloading during startup.

Database Lifetime
=================

The default ``connection_lifetime="pool"`` keeps file-backed thread-local
connections open until the pool closes. That keeps extension loading and other
physical-connection bootstrap work out of every short session.

Close the pool when the run finishes so DuckDB releases the file lock and
temporary files can be removed:

.. code-block:: python

   config = build_isolated_config(Path("isolated.duckdb"), Path("/opt/duckdb/extensions"))

   try:
       with config.provide_session() as db:
           db.execute("SELECT 1")
   finally:
       config.close_pool()

Concurrency
===========

Use one config and pool per isolated database. Each pool maintains its own
thread-local connection registry, and ``close_pool()`` reaps connections created
on worker threads as well as the calling thread. This preserves isolation while
allowing concurrent runs to use separate DuckDB files.

Use ``connection_lifetime="session"`` only when you deliberately need the legacy
same-file behavior where each successful session releases its connection
immediately. For isolated database files, prefer the default pool lifetime and
close the pool at the run boundary.
