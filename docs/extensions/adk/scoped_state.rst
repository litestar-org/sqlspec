============
Scoped State
============

SQLSpec follows the ADK scoped-state prefixes and persists them through the
session service boundary.

State Prefixes
==============

.. list-table::
   :header-rows: 1

   * - Prefix
     - Scope
     - Persistence
   * - ``app:``
     - All sessions for the same ``app_name``
     - Stored in ``adk_app_state``
   * - ``user:``
     - Sessions for the same ``app_name`` and ``user_id``
     - Stored in ``adk_user_state``
   * - ``temp:``
     - Current runtime only
     - Never written to storage
   * - *(no prefix)*
     - One session
     - Stored in ``adk_session.state``

Write Behavior
==============

``SQLSpecSessionService.create_session()`` and
``SQLSpecSessionService.append_event()`` strip ``temp:`` keys before
persistence. Durable keys are split into three buckets:

- ``app:`` keys are written through ``upsert_app_state()``.
- ``user:`` keys are written through ``upsert_user_state()``.
- Unprefixed keys are written to the session row.

``append_event_and_update_state()`` remains the store-level atomic boundary for
the event row and the session row. The scoped app/user writes are routed by the
service through the dedicated scoped-state store hooks.

Read Behavior
=============

``SQLSpecSessionService.get_session()`` reads the session state, app state, and
user state, then returns the merged ADK view. Raw store reads return only the
session row state, so direct database inspection shows ``app:`` and ``user:``
keys in their dedicated tables rather than in ``adk_session.state``.

.. code-block:: python

   session = await session_service.create_session(
       app_name="agent",
       user_id="user_1",
       state={
           "app:model": "gemini",
           "user:theme": "dark",
           "turn": 1,
           "temp:scratch": "...",
       },
   )

   assert session.state == {
       "app:model": "gemini",
       "user:theme": "dark",
       "turn": 1,
   }
