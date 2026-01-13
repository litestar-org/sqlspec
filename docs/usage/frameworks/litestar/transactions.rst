=============
Transactions
=============

The SQLSpec plugin supports two transaction modes: **autocommit** and **manual commit**.
Choose the mode that fits your application's error handling and rollback requirements.

Commit Modes
------------

**Autocommit (default)**
   Each statement commits automatically. Use this for read-heavy workloads or when
   you don't need atomic multi-statement operations.

**Manual Commit**
   You control when to commit or rollback. Use this for write operations that must
   succeed or fail together.

.. literalinclude:: /examples/frameworks/litestar/commit_modes.py
   :language: python
   :caption: ``commit modes``
   :start-after: # start-example
   :end-before: # end-example
   :dedent: 4
   :no-upgrade:

Rollback on Error
-----------------

In manual mode, uncaught exceptions trigger an automatic rollback before the response
is sent. This keeps your database consistent when handlers fail.

.. code-block:: python

   @post("/transfer")
   async def transfer(db: AsyncSession, data: TransferRequest) -> dict:
       await db.execute("UPDATE accounts SET balance = balance - :amount WHERE id = :from_id",
                        {"amount": data.amount, "from_id": data.from_account})
       await db.execute("UPDATE accounts SET balance = balance + :amount WHERE id = :to_id",
                        {"amount": data.amount, "to_id": data.to_account})
       await db.commit()  # Both updates succeed or both rollback
       return {"status": "transferred"}

Nested Transactions
-------------------

Use savepoints for nested transaction scopes. SQLSpec translates ``begin_nested()``
to savepoints on databases that support them.
