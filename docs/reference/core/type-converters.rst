===============
Type Converters
===============

Output and input type converters for transforming database values. Used by
drivers to convert between Python types and database-native representations.

.. currentmodule:: sqlspec.core.type_converter

Base Classes
============

Subclass :class:`BaseInputConverter` to add adapter-specific input conversion.
Implementations receive the driver dialect and register conversions for database
types that are not covered by the built-in converter functions below.

.. autoclass:: BaseInputConverter
   :members:
   :show-inheritance:

Built-in Converters
===================

.. autofunction:: convert_decimal

.. autofunction:: convert_iso_date

.. autofunction:: convert_iso_datetime

.. autofunction:: convert_iso_time

.. autofunction:: convert_json

.. autofunction:: convert_uuid

.. autofunction:: format_datetime_rfc3339

.. autofunction:: parse_datetime_rfc3339
