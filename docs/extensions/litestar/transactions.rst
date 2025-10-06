============
Transactions
============

The SQLSpec Litestar extension provides three transaction management modes: manual, autocommit, and autocommit with redirects.

Overview
========

Transaction modes control when database changes are committed or rolled back based on HTTP response status codes.

Commit Modes
============

Manual Mode (Default)
---------------------

Explicit transaction control in route handlers.

**Configuration:**

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://localhost/mydb"},
       extension_config={"litestar": {"commit_mode": "manual"}}
   )

**Usage:**

.. code-block:: python

   from litestar import post
   from sqlspec.adapters.asyncpg import AsyncpgDriver

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       async with db_session.begin_transaction():
           result = await db_session.execute(
               "INSERT INTO users (name) VALUES ($1) RETURNING id",
               data["name"]
           )
           return result.one()

**When to use**:

- Complex transactions spanning multiple operations
- Custom transaction isolation levels
- Explicit savepoints

Autocommit Mode
---------------

Automatic commit on 2XX status codes, rollback on others.

**Configuration:**

.. code-block:: python

   config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://localhost/mydb"},
       extension_config={"litestar": {"commit_mode": "autocommit"}}
   )

**Usage:**

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       # Automatically commits if response is 2XX
       # Automatically rolls back if response is 4XX or 5XX
       result = await db_session.execute(
           "INSERT INTO users (name) VALUES ($1) RETURNING id",
           data["name"]
       )
       return result.one()

**Commit conditions**:

- HTTP status 200-299
- Any status in ``extra_commit_statuses``

**Rollback conditions**:

- HTTP status 300+ (redirects and errors)
- Any status in ``extra_rollback_statuses``

**When to use**:

- Simple CRUD operations
- REST APIs with standard status codes
- Reduced boilerplate

Autocommit with Redirects
--------------------------

Commits on both 2XX and 3XX redirect status codes.

**Configuration:**

.. code-block:: python

   config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://localhost/mydb"},
       extension_config={
           "litestar": {"commit_mode": "autocommit_include_redirect"}
       }
   )

**Commit conditions**:

- HTTP status 200-399 (success + redirects)
- Any status in ``extra_commit_statuses``

**When to use**:

- Applications that redirect after successful operations
- Login flows with database updates before redirect

Custom Status Codes
===================

Fine-tune commit/rollback behavior:

.. code-block:: python

   config = AsyncpgConfig(
       pool_config={"dsn": "postgresql://localhost/mydb"},
       extension_config={
           "litestar": {
               "commit_mode": "autocommit",
               "extra_commit_statuses": {201, 204},  # Commit on created/no-content
               "extra_rollback_statuses": {409}      # Rollback on conflict
           }
       }
   )

Transaction Examples
====================

Multi-Step Transaction
----------------------

.. code-block:: python

   from litestar import post
   from sqlspec.adapters.asyncpg import AsyncpgDriver

   @post("/orders")
   async def create_order(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       async with db_session.begin_transaction():
           # Create order
           order_result = await db_session.execute(
               "INSERT INTO orders (user_id, total) VALUES ($1, $2) RETURNING id",
               data["user_id"],
               data["total"]
           )
           order_id = order_result.scalar()

           # Create order items
           for item in data["items"]:
               await db_session.execute(
                   "INSERT INTO order_items (order_id, product_id, quantity) VALUES ($1, $2, $3)",
                   order_id,
                   item["product_id"],
                   item["quantity"]
               )

           # Update inventory
           for item in data["items"]:
               await db_session.execute(
                   "UPDATE products SET stock = stock - $1 WHERE id = $2",
                   item["quantity"],
                   item["product_id"]
               )

           return {"order_id": order_id}

Custom Isolation Level
-----------------------

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConnection
   from litestar import post

   @post("/critical-operation")
   async def critical_operation(
       data: dict,
       db_connection: AsyncpgConnection
   ) -> dict:
       async with db_connection.transaction(isolation="serializable"):
           # Perform critical operation with serializable isolation
           result = await db_connection.fetchrow(
               "UPDATE accounts SET balance = balance + $1 WHERE id = $2 RETURNING balance",
               data["amount"],
               data["account_id"]
           )
           return {"new_balance": result["balance"]}

Error Handling
==============

Autocommit mode automatically rolls back on errors:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgDriver
   from litestar import post, Response, HTTPException
   from litestar.status_codes import HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> Response:
       try:
           result = await db_session.execute(
               "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id",
               data["name"],
               data["email"]
           )
           return Response(result.one(), status_code=201)
       except KeyError:
           # 400 triggers rollback
           raise HTTPException(
               status_code=HTTP_400_BAD_REQUEST,
               detail="Missing required fields"
           )
       except Exception as e:
           # 500 triggers rollback
           return Response(
               {"error": str(e)},
               status_code=HTTP_500_INTERNAL_SERVER_ERROR
           )

Best Practices
==============

Use Autocommit for Simple Operations
-------------------------------------

.. code-block:: python

   # Good: Simple CRUD with autocommit
   config = AsyncpgConfig(
       extension_config={"litestar": {"commit_mode": "autocommit"}}
   )

   from sqlspec.adapters.asyncpg import AsyncpgDriver

   @post("/users")
   async def create_user(data: dict, db_session: AsyncpgDriver) -> dict:
       result = await db_session.execute(
           "INSERT INTO users (name) VALUES ($1) RETURNING id",
           data["name"]
       )
       return result.one()

Use Manual for Complex Transactions
------------------------------------

.. code-block:: python

   # Good: Complex multi-table transaction with manual mode
   config = AsyncpgConfig(
       extension_config={"litestar": {"commit_mode": "manual"}}
   )

   from sqlspec.adapters.asyncpg import AsyncpgDriver

   @post("/complex-operation")
   async def complex_operation(
       data: dict,
       db_session: AsyncpgDriver
   ) -> dict:
       async with db_session.begin_transaction():
           # Multiple operations
           await db_session.execute("INSERT INTO table1 ...")
           await db_session.execute("UPDATE table2 ...")
           await db_session.execute("DELETE FROM table3 ...")
           return {"status": "success"}

Return Appropriate Status Codes
--------------------------------

.. code-block:: python

   from litestar import Response

   from sqlspec.adapters.asyncpg import AsyncpgDriver

   @post("/users")
   async def create_user(
       data: dict,
       db_session: AsyncpgDriver
   ) -> Response:
       result = await db_session.execute(
           "INSERT INTO users (name) VALUES ($1) RETURNING id",
           data["name"]
       )
       # 201 Created triggers commit in autocommit mode
       return Response(result.one(), status_code=201)

See Also
========

- :doc:`quickstart` - Get started with transactions
- :doc:`dependency_injection` - Inject database dependencies
- :doc:`api` - Complete API reference
- :doc:`/usage/drivers_and_querying` - Query execution details
