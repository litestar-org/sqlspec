=============
Transactions
=============

The SQLSpec plugin supports three transaction commit modes: ``manual``,
``autocommit``, and ``autocommit_include_redirect``.

Commit Modes
------------

``manual`` (default)
   SQLSpec manages connection lifecycle and cleanup at response completion, but leaves
   transaction control to your application logic. Use this when handlers need explicit
   commits or multi-step error recovery.

``autocommit``
   SQLSpec automatically commits transactions for successful HTTP responses (status
   codes 200–299) and issues a rollback for client or server errors (status codes 400+)
   or unhandled exceptions.

``autocommit_include_redirect``
   Extends ``autocommit`` behavior to also commit transactions when the response is a
   redirect (status codes 200–399).

Configure the commit mode under ``extension_config["litestar"]``:

.. literalinclude:: /examples/frameworks/litestar/commit_modes.py
   :language: python
   :caption: ``commit modes``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Custom Status Codes
-------------------

You can customize which HTTP statuses trigger commits or rollbacks using
``extra_commit_statuses`` and ``extra_rollback_statuses``:

.. code-block:: python

   from sqlspec.adapters.asyncpg import AsyncpgConfig

   config = AsyncpgConfig(
       connection_config={"dsn": "postgresql://localhost/app"},
       extension_config={
           "litestar": {
               "commit_mode": "autocommit",
               "extra_rollback_statuses": {409},
               "extra_commit_statuses": {207},
           }
       },
   )

Manual Transactions in Handlers
-------------------------------

In ``manual`` mode, handlers control transaction boundaries directly on the injected driver:

.. code-block:: python

   from litestar import post
   from pydantic import BaseModel
   from sqlspec.adapters.asyncpg import AsyncpgDriver


   class TransferRequest(BaseModel):
       from_account: int
       to_account: int
       amount: int


   @post("/transfer")
   async def transfer(db_session: AsyncpgDriver, data: TransferRequest) -> dict[str, str]:
       await db_session.execute(
           "UPDATE accounts SET balance = balance - :amount WHERE id = :from_id",
           amount=data.amount,
           from_id=data.from_account,
       )
       await db_session.execute(
           "UPDATE accounts SET balance = balance + :amount WHERE id = :to_id",
           amount=data.amount,
           to_id=data.to_account,
       )
       await db_session.commit()
       return {"status": "transferred"}

Savepoints
----------

SQLSpec drivers provide explicit savepoint management within active transactions:

.. code-block:: python

   await db_session.create_savepoint("sp1")
   try:
       await db_session.execute("INSERT INTO audit_log (event) VALUES (:event)", event="processed")
       await db_session.release_savepoint("sp1")
   except Exception:
       await db_session.rollback_to_savepoint("sp1")

Error Handling and 409 Conflict
-------------------------------

When a database operation raises :class:`~sqlspec.exceptions.IntegrityError` (such as a unique
constraint or foreign key violation), ``SQLSpecPlugin`` automatically translates it to an
HTTP 409 Conflict response with generic detail ``"Conflict"``. In ``autocommit`` mode,
this error status triggers an automatic rollback of the request transaction.

Custom handlers registered on the application or router for :class:`~sqlspec.exceptions.IntegrityError`
take precedence, while broader handlers (e.g. for :class:`~sqlspec.exceptions.SQLSpecError` or status 500)
receive the original exception.
