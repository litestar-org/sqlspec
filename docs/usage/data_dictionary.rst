===============
Data Dictionary
===============

SQLSpec's data dictionary is being rebuilt as a clean pre-1.0 replacement
interface. The old narrow metadata shape is not kept as a compatibility target;
adapter chapters preserve the same discovery capabilities while moving them
behind richer result and capability contracts.

Breaking Interface Change
=========================

The replacement contract uses structured metadata envelopes:

- ``MetadataCapabilityProfile`` reports support by domain and adapter.
- ``MetadataCapability`` distinguishes supported, unsupported, unknown, and
  not implemented domains.
- ``MetadataResult`` wraps every domain lookup, including unsupported domains,
  so callers never need to guess whether an empty list means "nothing exists" or
  "the database cannot answer this".
- Object detail models carry an ``ObjectIdentity`` with catalog, schema,
  object name, object type, dialect, quoted name, and source.
- DDL lookups return a ``DDLResult`` with native/generated/hybrid/lossy status
  instead of returning plain SQL text.

Applications using the old flat methods should plan to migrate to the
replacement domain methods and inspect ``MetadataResult.capability`` before
using ``MetadataResult.items``.

Capability Vocabulary
=====================

The stable support values are:

- ``supported``
- ``unsupported``
- ``unknown``
- ``not_implemented``

The stable fidelity values are:

- ``native``
- ``generated``
- ``hybrid``
- ``lossy``
- ``partial``
- ``transport_fallback``

Risk gates describe why metadata may be hidden, expensive, or unavailable:
``privileged``, ``billed``, ``expensive``, ``license_gated``, ``redacted``,
``managed_service_limited``, ``extension_required``, and ``version_gated``.

Migration Guidance
==================

Use capabilities first:

.. code-block:: python

   profile = db.data_dictionary.get_metadata_capabilities(db)
   tables = profile.get("tables")

   if tables.support == "supported":
       result = db.data_dictionary.get_table_details(db, "users")
       for item in result.items:
           ...

For unsupported domains, SQLSpec returns a structured result:

.. code-block:: python

   result = db.data_dictionary.get_privileges(db, "users")
   if result.capability.support == "unsupported":
       ...

System and performance metadata is disabled by default unless the adapter
chapter explicitly opts into a safe, redacted, privilege-aware implementation.
