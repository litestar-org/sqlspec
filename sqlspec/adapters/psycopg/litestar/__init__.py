"""Litestar integration for Psycopg adapter."""

from sqlspec.adapters.psycopg.litestar.store import PsycopgAsyncStore, PsycopgSyncStore

__all__ = ("PsycopgAsyncStore", "PsycopgSyncStore")
