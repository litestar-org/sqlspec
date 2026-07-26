========
OracleDB
========

Sync and async Oracle adapter using `python-oracledb <https://python-oracledb.readthedocs.io/>`_.
Features native pipeline mode for multi-statement batching, BLOB support, and
LOB coercion with byte-length thresholds.

Type Handling
=============

SQLSpec installs composable Oracle input and output handlers when a pooled
connection is initialized. The handlers preserve Python values where the
database has a matching native type and use explicit Oracle storage conventions
where it does not.

.. list-table:: Bind behavior
   :header-rows: 1
   :widths: 24 31 45

   * - Python value
     - Oracle bind/storage
     - Notes
   * - ``dict`` or a non-numeric ``list``/``tuple``
     - JSON storage selected for the server
     - Numeric sequences are reserved for VECTOR binding. An empty sequence is
       ambiguous and is not claimed automatically.
   * - :class:`~sqlspec.adapters.oracledb.OracleJson`
     - JSON storage selected for the server
     - Expresses JSON intent. It does not force ``DB_TYPE_JSON`` when the
       connected server does not expose native JSON storage.
   * - :class:`~sqlspec.adapters.oracledb.OracleClob`
     - ``DB_TYPE_CLOB``
     - Bypasses the automatic string-size threshold.
   * - :class:`~sqlspec.adapters.oracledb.OracleBlob`
     - ``DB_TYPE_BLOB``
     - Bypasses the automatic bytes-size threshold.
   * - :class:`uuid.UUID`
     - ``RAW(16)``
     - Enabled by ``enable_uuid_binary=True``.
   * - NumPy array or a numeric Python sequence
     - ``VECTOR``
     - Requires Oracle Database 23ai for VECTOR columns. Sparse vectors remain
       python-oracledb ``SparseVector`` values.

.. list-table:: Read behavior
   :header-rows: 1
   :widths: 27 31 42

   * - Oracle column
     - Python value
     - Notes
   * - native ``JSON``
     - ``dict`` or ``list``
     - python-oracledb performs native conversion. JSON numbers may be
       :class:`decimal.Decimal`.
   * - ``BLOB``/``CLOB``/character data marked ``IS JSON``
     - ``dict`` or ``list``
     - SQLSpec uses fetch metadata to decode JSON. Textual JSON number lanes
       produce ordinary ``int``/``float`` values.
   * - OSON ``BLOB``
     - ``dict`` or ``list``
     - Decoded through python-oracledb when OSON metadata and support are
       available.
   * - unconstrained ``BLOB``/``CLOB``
     - ``bytes``/``str`` or a LOB locator
     - JSON-looking contents are not decoded without JSON metadata.
   * - ``RAW(16)``
     - :class:`uuid.UUID`
     - Other RAW widths remain bytes.
   * - ``VECTOR``
     - NumPy array, ``list``, or ``array.array``
     - Controlled by ``vector_return_format``.

JSON Storage By Oracle Version
==============================

The server version selects the automatic JSON bind rung:

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Oracle Database
     - Automatic JSON bind
     - Coverage and constraints
   * - 21c and newer, including 23ai
     - native ``JSON`` / ``DB_TYPE_JSON``
     - The repository integration lane exercises 23ai. Native JSON numbers may
       be returned as :class:`decimal.Decimal`.
   * - 12c through 20c, including 18c and 19c
     - ``BLOB`` with ``IS JSON``
     - The automated compatibility lane uses Oracle 18c because the pinned
       pytest-databases release does not provide a 19c service fixture.
   * - 11g and earlier
     - ``CLOB``
     - Capability fallback only; it is not part of the automated service
       matrix.

``BLOB IS JSON`` is the preferred pre-native JSON storage because UTF-8 byte
storage avoids CLOB character-set conversion and typically uses less space for
JSON. Keep CLOB storage as an explicit compatibility or application choice.

Explicit ``CLOB CHECK (payload IS JSON)`` columns remain readable through
metadata-driven conversion on supported servers. For ``BLOB IS JSON`` storage,
SQLSpec serializes direct Python JSON values to UTF-8 and binds a BLOB locator;
callers do not need to provide serialized strings.

Driver Feature Escape Hatches
=============================

Pass these keys through ``driver_features`` on
:class:`~sqlspec.adapters.oracledb.OracleSyncConfig` or
:class:`~sqlspec.adapters.oracledb.OracleAsyncConfig`:

.. list-table::
   :header-rows: 1
   :widths: 32 23 45

   * - Key
     - Default
     - Effect
   * - ``fetch_lobs``
     - ``False``
     - Return supported LOB values directly; set ``True`` to request native
       locators.
   * - ``fetch_decimals``
     - driver default
     - Request Decimal NUMBER results where python-oracledb supports them.
   * - ``enable_uuid_binary``
     - ``True``
     - Convert between :class:`uuid.UUID` and ``RAW(16)``.
   * - ``enable_numpy_vectors``
     - whether NumPy is installed
     - Enable NumPy VECTOR conversion.
   * - ``vector_return_format``
     - ``"numpy"`` with NumPy, otherwise ``"list"``
     - Choose ``"numpy"``, ``"list"``, or ``"array"`` for dense VECTOR
       results.
   * - ``oracle_varchar2_byte_limit``
     - ``4000``
     - Route larger UTF-8 strings to CLOB; installations using
       ``MAX_STRING_SIZE=EXTENDED`` may choose ``32767``.
   * - ``oracle_raw_byte_limit``
     - ``2000``
     - Route larger byte payloads to BLOB.
   * - ``arraysize`` / ``prefetchrows``
     - python-oracledb defaults
     - Override per-cursor fetch buffering.
   * - ``enable_lowercase_column_names``
     - ``True``
     - Normalize implicit uppercase Oracle identifiers for result mappings.

LOB And JSON Fetching
=====================

Oracle configurations default ``fetch_lobs`` to ``False``. With modern
``python-oracledb`` this returns supported LOB values under Oracle's 1 GB
direct-fetch ceiling directly as ``str`` or ``bytes`` for normal SELECTs,
streaming reads, and Arrow exports. SQLSpec still materializes readable locators
when Oracle returns one, so buffered results and schema hydration do not expose
driver handles by default.

Pass ``fetch_lobs=True`` on a query when application code needs native Oracle
LOB locators, for example in a streaming workflow that wants to control when a
large value is read.

JSON fetch conversion is metadata-driven:

* native ``JSON`` columns are returned by ``python-oracledb``;
* ``IS JSON`` CLOB/BLOB/VARCHAR2 columns are decoded through Oracle fetch
  metadata;
* OSON BLOB values are decoded through Oracle's OSON support when the server and
  driver expose it.

Unconstrained CLOB or BLOB columns are returned as text or bytes even when their
contents look like JSON. Add an Oracle JSON type or ``IS JSON`` constraint when
you want automatic JSON decoding.

MERGE Upserts
=============

Oracle uses ``MERGE`` for an update-or-insert operation. PostgreSQL
``INSERT ... ON CONFLICT`` syntax is not valid Oracle SQL, and SQLSpec does not
rewrite it into ``MERGE``. For a single row, select the named bind values from
``DUAL`` and use the same source aliases in both branches::

    merge_widget = """
    MERGE INTO widget t
    USING (
        SELECT :sku AS sku, :name AS name, :quantity AS quantity
        FROM DUAL
    ) s
    ON (t.sku = s.sku)
    WHEN MATCHED THEN
        UPDATE SET
            t.name = s.name,
            t.quantity = s.quantity,
            t.updated_at = SYSTIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (id, sku, name, quantity, created_at, updated_at)
        VALUES (
            widget_seq.NEXTVAL,
            s.sku,
            s.name,
            s.quantity,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        )
    """

    await session.execute(
        merge_widget,
        {"sku": "W-100", "name": "Widget", "quantity": 3},
    )

Do not add ``RETURNING`` to this ``MERGE``. When the caller needs an ID
generated by the insert branch, select it by the same unique key before the
transaction is committed::

    widget_id = await session.select_value(
        "SELECT id FROM widget WHERE sku = :sku",
        {"sku": "W-100"},
    )

Keeping the ``MERGE`` and follow-up ``SELECT`` in one SQLSpec session preserves
their transaction boundary. For large LOB values, the adapter's Litestar
session store uses the same pattern to merge an ``EMPTY_BLOB()``, select it
``FOR UPDATE``, and write through the returned locator.

.. _oracledb-extension-storage-options:

Extension Table Storage Options
===============================

Oracle ADK, durable event, and Litestar session tables support the same
opt-in storage concepts under their extension configuration: ``in_memory``,
``compression``, ``partitioning``, and table options. For example, an events
queue can use Advanced Compression and monthly interval partitions::

    extension_config = {
        "events": {
            "compression": {"enabled": True, "algorithm": "advanced"},
            "partitioning": {
                "strategy": "range",
                "partition_key": "available_at",
                "interval": "month",
            },
            "table_options": "TABLESPACE event_data",
        }
    }

Use the same keys under ``litestar``; range partitioning defaults to
``expires_at``. Under ``adk``, per-table options use names such as
``session_table_options``, ``events_table_options``, and
``memory_table_options``. ADK partition settings can likewise override a
specific table key with ``session_partition_key``, ``events_partition_key``,
or the corresponding state or memory key.

SQLSpec resolves Oracle Partitioning, Advanced Compression, Basic Compression,
and Database In-Memory availability once per connection pool through the data
dictionary. If the option catalog is inaccessible or a requested feature is not
available, SQLSpec logs a structured warning and creates the table without that
optimization. User-provided table options are still emitted because they are
application DDL rather than a capability-detected Oracle option.

SQLSpec does not automatically add ``SECUREFILE`` LOB compression. Its safety
also depends on tablespace segment-space management and database-level
``DB_SECUREFILE`` policy, which cannot be established from the option catalog
alone. Add a reviewed LOB clause through the table-options setting when the
deployment guarantees those prerequisites.

Sync Configuration
==================

.. autoclass:: sqlspec.adapters.oracledb.OracleSyncConfig
   :members:
   :show-inheritance:

Async Configuration
===================

.. autoclass:: sqlspec.adapters.oracledb.OracleAsyncConfig
   :members:
   :show-inheritance:

Sync Driver
===========

.. autoclass:: sqlspec.adapters.oracledb.OracleSyncDriver
   :members:
   :show-inheritance:

Async Driver
============

.. autoclass:: sqlspec.adapters.oracledb.OracleAsyncDriver
   :members:
   :show-inheritance:

Data Dictionary
===============

.. autoclass:: sqlspec.adapters.oracledb.data_dictionary.OracleVersionInfo
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.oracledb.data_dictionary.OracledbSyncDataDictionary
   :members:
   :show-inheritance:

.. autoclass:: sqlspec.adapters.oracledb.data_dictionary.OracledbAsyncDataDictionary
   :members:
   :show-inheritance:
