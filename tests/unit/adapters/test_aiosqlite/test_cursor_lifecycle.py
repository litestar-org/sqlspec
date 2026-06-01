"""Unit tests for AiosqliteCursor cleanup on exception paths."""

import sqlite3

import aiosqlite
import pytest

from sqlspec.adapters.aiosqlite._typing import AiosqliteCursor


async def test_cursor_closed_after_normal_exit() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        async with AiosqliteCursor(conn) as cursor:
            await cursor.execute("SELECT 1")

        with pytest.raises(sqlite3.ProgrammingError, match="closed cursor"):
            cursor._cursor.execute("SELECT 1")
    finally:
        await conn.close()


async def test_cursor_closed_after_exception_in_block() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        captured_cursor = None
        with pytest.raises(aiosqlite.OperationalError):
            async with AiosqliteCursor(conn) as cursor:
                captured_cursor = cursor
                await cursor.execute("SELECT * FROM nonexistent_table_xyz")

        assert captured_cursor is not None
        with pytest.raises(sqlite3.ProgrammingError, match="closed cursor"):
            captured_cursor._cursor.execute("SELECT 1")
    finally:
        await conn.close()


async def test_cursor_closed_after_generic_exception_in_block() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        captured_cursor = None
        with pytest.raises(ValueError, match="simulated error"):
            async with AiosqliteCursor(conn) as cursor:
                captured_cursor = cursor
                raise ValueError("simulated error")

        assert captured_cursor is not None
        with pytest.raises(sqlite3.ProgrammingError, match="closed cursor"):
            captured_cursor._cursor.execute("SELECT 1")
    finally:
        await conn.close()


async def test_aexit_does_not_suppress_exception() -> None:
    conn = await aiosqlite.connect(":memory:")
    try:
        with pytest.raises(aiosqlite.OperationalError):
            async with AiosqliteCursor(conn) as cursor:
                await cursor.execute("SELECT * FROM nonexistent_table_xyz")
    finally:
        await conn.close()
