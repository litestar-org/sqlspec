========================
Oracle Database Backend
========================

Overview
========

Oracle Database is an enterprise-grade relational database system designed for mission-critical applications with high performance, reliability, and advanced features. The SQLSpec ADK integration provides intelligent version-specific JSON storage that automatically adapts to your Oracle version.

**Key Features:**

- **Enterprise-Grade**: ACID compliance, advanced security, and high availability
- **Version-Adaptive JSON Storage**: Automatic detection and optimization for Oracle 21c, 12c, and legacy versions
- **Timezone-Aware**: TIMESTAMP WITH TIME ZONE for accurate global timestamps
- **Connection Pooling**: Built-in pool management for optimal performance
- **Thin & Thick Modes**: Choose between pure Python or Oracle Client deployment
- **Advanced Data Types**: BLOB, CLOB, and native JSON support

**Ideal Use Cases:**

- Enterprise AI agent deployments requiring high reliability
- Organizations with existing Oracle infrastructure
- Applications requiring advanced security and compliance features
- Multi-region deployments with timezone awareness
- Mission-critical systems requiring 24/7 availability

Installation
============

Oracle supports two deployment modes:

Thin Mode (Pure Python - Recommended)
--------------------------------------

Install SQLSpec with Oracle thin mode support:

.. code-block:: bash

   pip install sqlspec[oracledb] google-genai
   # or
   uv pip install sqlspec[oracledb] google-genai

**Advantages:**

- No Oracle Client installation required
- Smaller deployment footprint
- Easier containerization
- Cross-platform compatibility
- Suitable for most use cases

Thick Mode (Oracle Client)
---------------------------

For advanced features requiring Oracle Client libraries:

.. code-block:: bash

   # 1. Install Oracle Instant Client
   # Download from: https://www.oracle.com/database/technologies/instant-client/downloads.html

   # 2. Install SQLSpec with Oracle support
   pip install sqlspec[oracledb] google-genai

.. code-block:: python

   import oracledb

   # Initialize thick mode (before creating connections)
   oracledb.init_oracle_client(
       lib_dir="/path/to/instantclient"
   )

**Required For:**

- Kerberos authentication
- LDAP-based authentication
- Advanced Oracle Wallet features
- Some legacy Oracle features

.. tip::

   Start with **thin mode**. Switch to thick mode only if you need specific features.
   Thin mode covers 95% of use cases with zero installation overhead.

Quick Start
===========

Async Store (Recommended)
--------------------------

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleAsyncConfig
   from sqlspec.adapters.oracledb.adk import OracleAsyncADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   # Configure Oracle connection
   config = OracleAsyncConfig(
       pool_config={
           "user": "agent_user",
           "password": "secure_password",
           "dsn": "oracle.example.com:1521/XEPDB1",
           "min": 2,
           "max": 10,
       }
   )

   # Create store and initialize tables
   store = OracleAsyncADKStore(config)
   await store.create_tables()

   # Use with session service
   service = SQLSpecSessionService(store)

   # Create session
   session = await service.create_session(
       app_name="enterprise_agent",
       user_id="user_123",
       state={"context": "active", "priority": "high"}
   )

Sync Store
----------

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleSyncConfig
   from sqlspec.adapters.oracledb.adk import OracleSyncADKStore

   # Configure Oracle connection
   config = OracleSyncConfig(
       pool_config={
           "user": "agent_user",
           "password": "secure_password",
           "dsn": "oracle.example.com:1521/XEPDB1",
           "min": 2,
           "max": 10,
       }
   )

   # Create store and initialize tables
   store = OracleSyncADKStore(config)
   store.create_tables()

   # Use directly
   session = store.create_session(
       session_id="unique_id",
       app_name="enterprise_agent",
       user_id="user_123",
       state={"context": "active"}
   )

Configuration
=============

Connection String Formats
--------------------------

Oracle supports multiple DSN (Data Source Name) formats:

**Easy Connect (Recommended):**

.. code-block:: python

   config = OracleAsyncConfig(
       pool_config={
           "user": "agent_user",
           "password": "secure_password",
           "dsn": "hostname:1521/service_name",
       }
   )

**Easy Connect Plus:**

.. code-block:: python

   config = OracleAsyncConfig(
       pool_config={
           "user": "agent_user",
           "password": "secure_password",
           "dsn": "tcps://hostname:2484/service_name?ssl_server_cert_dn=CN=server",
       }
   )

**TNS Connect Descriptor:**

.. code-block:: python

   config = OracleAsyncConfig(
       pool_config={
           "user": "agent_user",
           "password": "secure_password",
           "dsn": """(DESCRIPTION=
                       (ADDRESS=(PROTOCOL=TCP)(HOST=hostname)(PORT=1521))
                       (CONNECT_DATA=(SERVICE_NAME=service_name)))""",
       }
   )

**TNS Alias (from tnsnames.ora):**

.. code-block:: python

   config = OracleAsyncConfig(
       pool_config={
           "user": "agent_user",
           "password": "secure_password",
           "dsn": "PROD_DB",  # Name from tnsnames.ora
       }
   )

Connection Pool Configuration
------------------------------

Oracle connection pooling is **mandatory** for production:

.. code-block:: python

   config = OracleAsyncConfig(
       pool_config={
           "user": "agent_user",
           "password": "secure_password",
           "dsn": "oracle.example.com:1521/XEPDB1",
           "min": 2,                 # Minimum connections (keep warm)
           "max": 10,                # Maximum connections
           "increment": 1,           # How many to add when growing
           "threaded": True,         # Thread-safe pool
           "getmode": oracledb.POOL_GETMODE_WAIT,
       }
   )

**Pool Parameters:**

- ``min``: Minimum pool size (keep connections warm)
- ``max``: Maximum pool size (prevent resource exhaustion)
- ``increment``: How many connections to add when scaling up
- ``threaded``: Enable thread safety (required for multi-threaded apps)
- ``getmode``: ``WAIT`` (block until available) or ``NOWAIT`` (error if full)

Custom Table Names
------------------

.. code-block:: python

   store = OracleAsyncADKStore(
       config,
       session_table="agent_sessions",
       events_table="agent_events"
   )

Schema
======

Version-Adaptive JSON Storage
------------------------------

The Oracle ADK store **automatically detects** your Oracle version and uses the optimal JSON storage type:

.. list-table:: JSON Storage Evolution
   :header-rows: 1
   :widths: 20 30 50

   * - Oracle Version
     - Storage Type
     - Details
   * - **21c+** (compatible >= 20)
     - Native JSON
     - ``state JSON NOT NULL`` - Best performance, native validation
   * - **12c - 20c**
     - BLOB with JSON constraint
     - ``state BLOB CHECK (state IS JSON) NOT NULL`` - Recommended by Oracle
   * - **11g and earlier**
     - BLOB (plain)
     - ``state BLOB NOT NULL`` - No validation, maximum compatibility

.. note::

   Version detection happens **once** at table creation by querying:

   - ``product_component_version`` for Oracle version
   - ``v$parameter`` for compatibility setting

   The result is cached to avoid repeated checks.

Sessions Table
--------------

**Oracle 21c+ (Native JSON):**

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR2(128) PRIMARY KEY,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       state JSON NOT NULL,
       create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
       update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
   );

   CREATE INDEX idx_adk_sessions_app_user
       ON adk_sessions(app_name, user_id);

   CREATE INDEX idx_adk_sessions_update_time
       ON adk_sessions(update_time DESC);

**Oracle 12c - 20c (BLOB with JSON Constraint):**

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR2(128) PRIMARY KEY,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       state BLOB CHECK (state IS JSON) NOT NULL,
       create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
       update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
   );

**Oracle 11g and earlier (BLOB):**

.. code-block:: sql

   CREATE TABLE adk_sessions (
       id VARCHAR2(128) PRIMARY KEY,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       state BLOB NOT NULL,
       create_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
       update_time TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
   );

Events Table
------------

**Oracle 21c+ (Native JSON):**

.. code-block:: sql

   CREATE TABLE adk_events (
       id VARCHAR2(128) PRIMARY KEY,
       session_id VARCHAR2(128) NOT NULL,
       app_name VARCHAR2(128) NOT NULL,
       user_id VARCHAR2(128) NOT NULL,
       invocation_id VARCHAR2(256),
       author VARCHAR2(256),
       actions BLOB,
       long_running_tool_ids_json CLOB,
       branch VARCHAR2(256),
       timestamp TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
       content JSON,
       grounding_metadata JSON,
       custom_metadata JSON,
       partial NUMBER(1),
       turn_complete NUMBER(1),
       interrupted NUMBER(1),
       error_code VARCHAR2(256),
       error_message VARCHAR2(1024),
       CONSTRAINT fk_adk_events_session FOREIGN KEY (session_id)
           REFERENCES adk_sessions(id) ON DELETE CASCADE
   );

   CREATE INDEX idx_adk_events_session
       ON adk_events(session_id, timestamp ASC);

**Oracle 12c - 20c (BLOB with JSON Constraint):**

.. code-block:: sql

   CREATE TABLE adk_events (
       id VARCHAR2(128) PRIMARY KEY,
       session_id VARCHAR2(128) NOT NULL,
       -- ... other fields ...
       content BLOB CHECK (content IS JSON),
       grounding_metadata BLOB CHECK (grounding_metadata IS JSON),
       custom_metadata BLOB CHECK (custom_metadata IS JSON),
       -- ... rest of schema ...
   );

Data Type Mappings
------------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Python Type
     - Oracle Type
     - Notes
   * - ``str``
     - ``VARCHAR2(n)``
     - Text fields
   * - ``dict``
     - ``JSON`` / ``BLOB``
     - Version-specific
   * - ``bytes``
     - ``BLOB``
     - Actions field
   * - ``bool``
     - ``NUMBER(1)``
     - 0 = False, 1 = True
   * - ``datetime``
     - ``TIMESTAMP WITH TIME ZONE``
     - Timezone-aware
   * - ``None``
     - ``NULL``
     - Nullable fields

.. important::

   **Boolean Conversion**: Oracle doesn't have a native BOOLEAN type. The store automatically converts:

   - ``True`` → ``1``
   - ``False`` → ``0``
   - ``None`` → ``NULL``

Usage Patterns
==============

Version Detection
-----------------

The store automatically detects and logs the Oracle version:

.. code-block:: python

   store = OracleAsyncADKStore(config)
   await store.create_tables()

   # Logs output:
   # INFO: Detected Oracle 21.3.0.0.0 with compatible >= 20, using JSON_NATIVE
   # OR
   # INFO: Detected Oracle 19.8.0.0.0, using BLOB_JSON (recommended)
   # OR
   # INFO: Detected Oracle 11.2.0.4.0 (pre-12c), using BLOB_PLAIN

Session Management
------------------

.. code-block:: python

   # Create session
   session = await store.create_session(
       session_id="unique_session_id",
       app_name="enterprise_agent",
       user_id="user_123",
       state={"context": "active", "workflow": "approval"}
   )

   # Get session
   session = await store.get_session("unique_session_id")

   # Update state (replaces entire state dict)
   await store.update_session_state(
       "unique_session_id",
       {"context": "completed", "result": "approved"}
   )

   # List sessions for user
   sessions = await store.list_sessions("enterprise_agent", "user_123")

   # Delete session (cascades to events)
   await store.delete_session("unique_session_id")

Event Management
----------------

.. code-block:: python

   from datetime import datetime, timezone

   # Append event
   event = EventRecord(
       id="event_id",
       session_id="session_id",
       app_name="enterprise_agent",
       user_id="user_123",
       author="user",
       actions=b"pickled_actions_data",
       timestamp=datetime.now(timezone.utc),
       content={"message": "User input"},
       partial=False,
       turn_complete=True,
   )

   await store.append_event(event)

   # Get events for session
   events = await store.get_events("session_id")

   # Get recent events only
   from datetime import timedelta
   yesterday = datetime.now(timezone.utc) - timedelta(days=1)
   recent_events = await store.get_events(
       "session_id",
       after_timestamp=yesterday,
       limit=100
   )

LOB Handling
------------

Oracle LOBs (Large Objects) require special handling:

.. code-block:: python

   # Store handles LOB reads automatically
   session = await store.get_session("session_id")
   state = session["state"]  # Automatically deserialized from LOB

   # Large JSON documents (> 4KB) are efficiently stored as BLOBs
   large_state = {
       "conversation_history": [...],  # Large list
       "user_context": {...},
   }
   await store.update_session_state("session_id", large_state)

Performance Considerations
==========================

JSON Storage Types Performance
-------------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 25 25 25

   * - Storage Type
     - Read Performance
     - Write Performance
     - Validation
   * - **Native JSON** (21c+)
     - Excellent
     - Excellent
     - Built-in
   * - **BLOB + IS JSON** (12c+)
     - Very Good
     - Very Good
     - Database-enforced
   * - **BLOB Plain** (11g)
     - Good
     - Good
     - Application-level

.. tip::

   **Upgrade Recommendation**: If using Oracle 12c-20c, upgrade to 21c+ for native JSON performance improvements.

Connection Pooling Impact
--------------------------

**Without Pooling** (❌ Not Recommended):

- Each query creates a new connection
- Significant overhead (100-500ms per connection)
- Resource exhaustion under load

**With Pooling** (✅ Recommended):

- Reuse warm connections (< 1ms overhead)
- Predictable resource usage
- Better performance under concurrent load

.. code-block:: python

   # Good: Reuse pooled connection
   async with config.provide_connection() as conn:
       cursor = conn.cursor()
       await cursor.execute(query1)
       await cursor.execute(query2)  # Same connection
       await conn.commit()

Statement Caching
-----------------

Oracle automatically caches prepared statements:

.. code-block:: python

   # Connection-level statement cache
   connection.stmtcachesize = 40  # Default is 20

Batch Operations
----------------

For bulk event inserts, consider batch operations:

.. code-block:: python

   # Instead of: (Slow)
   for event in events:
       await store.append_event(event)

   # Consider: (Faster - if implementing)
   # await store.append_events_batch(events)

Best Practices
==============

Oracle Version Considerations
------------------------------

**Oracle 21c+:**

- ✅ Use native JSON features
- ✅ Leverage JSON query syntax
- ✅ Benefit from automatic indexing

**Oracle 12c - 20c:**

- ✅ BLOB storage with validation is efficient
- ⚠️ Consider upgrading to 21c for JSON improvements
- ✅ Check constraints ensure data integrity

**Oracle 11g and earlier:**

- ⚠️ No automatic JSON validation
- ⚠️ Consider upgrading for security and features
- ✅ Application-level validation still works

Thin vs Thick Mode
-------------------

**Prefer Thin Mode When:**

- ✅ Deploying in containers (Docker, Kubernetes)
- ✅ Using cloud environments
- ✅ Want zero-install deployment
- ✅ Standard authentication (user/password)

**Use Thick Mode When:**

- ❌ Require Kerberos authentication
- ❌ Need LDAP-based authentication
- ❌ Using Oracle Wallet
- ❌ Need specific legacy features

Security Best Practices
------------------------

.. code-block:: python

   # 1. Use environment variables for credentials
   import os

   config = OracleAsyncConfig(
       pool_config={
           "user": os.environ["ORACLE_USER"],
           "password": os.environ["ORACLE_PASSWORD"],
           "dsn": os.environ["ORACLE_DSN"],
       }
   )

   # 2. Use Oracle Wallet (thick mode)
   oracledb.init_oracle_client()
   config = OracleAsyncConfig(
       pool_config={
           "dsn": "wallet_alias",
           # No user/password needed - from wallet
       }
   )

   # 3. Limit connection pool size
   config = OracleAsyncConfig(
       pool_config={
           "max": 10,  # Prevent resource exhaustion
       }
   )

Error Handling
--------------

.. code-block:: python

   from oracledb import DatabaseError

   try:
       session = await store.get_session("session_id")
   except DatabaseError as e:
       error_obj = e.args[0] if e.args else None
       if error_obj:
           if error_obj.code == 942:  # ORA-00942: Table does not exist
               await store.create_tables()
           elif error_obj.code == 1:  # ORA-00001: Unique constraint violated
               # Handle duplicate
               pass

Common Oracle Error Codes
--------------------------

- **ORA-00001**: Unique constraint violation
- **ORA-00054**: Resource busy (lock contention)
- **ORA-00942**: Table or view does not exist
- **ORA-01017**: Invalid username/password
- **ORA-12541**: TNS:no listener

Use Cases
=========

Enterprise AI Agent Platform
-----------------------------

.. code-block:: python

   from sqlspec.adapters.oracledb import OracleAsyncConfig
   from sqlspec.adapters.oracledb.adk import OracleAsyncADKStore
   from sqlspec.extensions.adk import SQLSpecSessionService

   # Production configuration
   config = OracleAsyncConfig(
       pool_config={
           "user": os.environ["ORACLE_USER"],
           "password": os.environ["ORACLE_PASSWORD"],
           "dsn": "prod-oracle.example.com:1521/PROD",
           "min": 5,
           "max": 20,
           "threaded": True,
       }
   )

   store = OracleAsyncADKStore(config)
   await store.create_tables()

   service = SQLSpecSessionService(store)

   # Handle thousands of concurrent sessions
   async def handle_user_request(user_id: str, message: str):
       session = await service.get_or_create_session(
           app_name="enterprise_assistant",
           user_id=user_id,
       )
       # Process with ADK
       # ...

Multi-Region Deployment
-----------------------

Oracle's timezone support ensures correct timestamps across regions:

.. code-block:: python

   from datetime import datetime, timezone

   # Store creates events with timezone-aware timestamps
   event = EventRecord(
       id="event_id",
       session_id="session_id",
       timestamp=datetime.now(timezone.utc),  # UTC
       # ...
   )

   await store.append_event(event)

   # Timestamps are preserved with timezone information
   events = await store.get_events("session_id")
   for event in events:
       local_time = event["timestamp"].astimezone()  # Convert to local

High-Availability Setup
-----------------------

.. code-block:: python

   # Oracle RAC (Real Application Clusters)
   config = OracleAsyncConfig(
       pool_config={
           "user": "agent_user",
           "password": "secure_password",
           "dsn": """(DESCRIPTION=
                       (ADDRESS_LIST=
                         (ADDRESS=(PROTOCOL=TCP)(HOST=node1)(PORT=1521))
                         (ADDRESS=(PROTOCOL=TCP)(HOST=node2)(PORT=1521))
                         (LOAD_BALANCE=yes)
                         (FAILOVER=yes))
                       (CONNECT_DATA=(SERVICE_NAME=PROD)))""",
       }
   )

Troubleshooting
===============

Version Detection Issues
------------------------

If version detection fails:

.. code-block:: python

   # Check Oracle version manually
   async with config.provide_connection() as conn:
       cursor = conn.cursor()
       await cursor.execute("""
           SELECT version FROM product_component_version
           WHERE product LIKE 'Oracle%Database%'
       """)
       version = await cursor.fetchone()
       print(f"Oracle version: {version[0]}")

**Solution**: The store defaults to BLOB_JSON (safe for 12c+) if detection fails.

JSON Storage Problems
---------------------

**Symptom**: ``ORA-02290: check constraint violated``

**Cause**: Invalid JSON in BLOB with ``IS JSON`` constraint.

**Solution**: Ensure data is valid JSON before storing:

.. code-block:: python

   import json

   # Validate JSON
   state = {"key": "value"}
   json.dumps(state)  # Raises exception if invalid

   await store.update_session_state("session_id", state)

Connection Errors
-----------------

**ORA-12541: TNS:no listener**

**Solutions**:

1. Verify Oracle listener is running: ``lsnrctl status``
2. Check firewall rules
3. Verify DSN format

**ORA-01017: Invalid username/password**

**Solutions**:

1. Verify credentials
2. Check user account is unlocked: ``ALTER USER agent_user ACCOUNT UNLOCK;``
3. Verify user has necessary privileges

Required Privileges
-------------------

Grant minimum required privileges:

.. code-block:: sql

   -- Create user
   CREATE USER agent_user IDENTIFIED BY secure_password;

   -- Grant basic privileges
   GRANT CREATE SESSION TO agent_user;
   GRANT CREATE TABLE TO agent_user;
   GRANT CREATE INDEX TO agent_user;

   -- Grant quota on tablespace
   ALTER USER agent_user QUOTA UNLIMITED ON USERS;

   -- Grant privileges on tables (if already created)
   GRANT SELECT, INSERT, UPDATE, DELETE ON adk_sessions TO agent_user;
   GRANT SELECT, INSERT, UPDATE, DELETE ON adk_events TO agent_user;

Comparison with Other Backends
===============================

Oracle vs PostgreSQL
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Feature
     - Oracle
     - PostgreSQL
   * - **JSON Storage**
     - Native JSON (21c+), BLOB (12c+)
     - Native JSONB with GIN indexes
   * - **Enterprise Features**
     - RAC, Data Guard, Partitioning
     - Streaming replication, logical replication
   * - **Licensing**
     - Commercial (paid)
     - Open source (free)
   * - **Deployment**
     - Complex setup
     - Simpler setup
   * - **Performance**
     - Excellent (enterprise-tuned)
     - Excellent (open source)
   * - **Best For**
     - Existing Oracle shops, enterprise
     - New deployments, cost-sensitive

Oracle vs DuckDB
----------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Feature
     - Oracle
     - DuckDB
   * - **Deployment**
     - Client-server
     - Embedded (single file)
   * - **Concurrency**
     - Excellent
     - Limited writes
   * - **Use Case**
     - Production AI agents
     - Development, analytics
   * - **Setup**
     - Complex
     - Zero config
   * - **Cost**
     - Commercial license
     - Free, open source

When to Choose Oracle
---------------------

**Choose Oracle When:**

✅ Already using Oracle infrastructure
✅ Require enterprise support and SLAs
✅ Need advanced HA features (RAC, Data Guard)
✅ Compliance requires certified databases
✅ Multi-region deployments with global transactions

**Choose Alternatives When:**

❌ Starting fresh (use PostgreSQL)
❌ Cost-sensitive (use PostgreSQL)
❌ Development/testing (use DuckDB or SQLite)
❌ Small-scale deployment (use PostgreSQL or DuckDB)

API Reference
=============

Async Store
-----------

.. autoclass:: sqlspec.adapters.oracledb.adk.OracleAsyncADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

Sync Store
----------

.. autoclass:: sqlspec.adapters.oracledb.adk.OracleSyncADKStore
   :members:
   :inherited-members:
   :show-inheritance:
   :no-index:

See Also
========

- :doc:`../quickstart` - Quick start guide
- :doc:`../adapters` - Adapter comparison
- :doc:`../schema` - Database schema details
- `python-oracledb Documentation <https://python-oracledb.readthedocs.io/>`_ - Official driver documentation
- `Oracle Database Documentation <https://docs.oracle.com/en/database/>`_ - Oracle Database guides
- `Oracle JSON Developer's Guide <https://docs.oracle.com/en/database/oracle/oracle-database/21/adjsn/>`_ - JSON features
