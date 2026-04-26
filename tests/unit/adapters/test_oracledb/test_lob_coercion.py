"""Unit tests for Oracle LOB parameter coercion."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec.adapters.oracledb.core import coerce_large_parameters_async, coerce_large_parameters_sync

# --- Fixtures ---

CLOB_TYPE = "DB_TYPE_CLOB"
BLOB_TYPE = "DB_TYPE_BLOB"
VARCHAR2_LIMIT = 4000
RAW_LIMIT = 2000


@pytest.fixture
def sync_connection() -> MagicMock:
    conn = MagicMock()
    conn.createlob.return_value = MagicMock(name="LOB")
    return conn


@pytest.fixture
def async_connection() -> AsyncMock:
    conn = AsyncMock()
    conn.createlob.return_value = MagicMock(name="LOB")
    return conn


# --- Sync Tests ---


class TestCoerceLargeParametersSync:
    def test_none_parameters_passthrough(self, sync_connection: MagicMock) -> None:
        result = coerce_large_parameters_sync(
            sync_connection,
            None,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        assert result is None

    def test_list_parameters_passthrough(self, sync_connection: MagicMock) -> None:
        params = ["a", "b"]
        result = coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        assert result is params  # unchanged

    def test_string_under_threshold_no_coercion(self, sync_connection: MagicMock) -> None:
        params = {"name": "x" * 100}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_not_called()
        assert params["name"] == "x" * 100

    def test_string_exactly_at_threshold_no_coercion(self, sync_connection: MagicMock) -> None:
        params = {"name": "a" * 4000}  # exactly 4000 bytes (ASCII)
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_not_called()

    def test_string_over_threshold_becomes_clob(self, sync_connection: MagicMock) -> None:
        params = {"content": "a" * 4001}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_called_once_with(CLOB_TYPE, "a" * 4001)
        assert params["content"] is sync_connection.createlob.return_value

    def test_multibyte_string_under_charcount_but_over_bytecount(self, sync_connection: MagicMock) -> None:
        """A string with 2000 CJK chars = 6000 UTF-8 bytes > 4000 byte limit."""
        # Each CJK character is 3 bytes in UTF-8
        value = "\u4e00" * 2000  # 2000 chars, 6000 bytes
        assert len(value) == 2000
        assert len(value.encode("utf-8")) == 6000
        params = {"content": value}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_called_once_with(CLOB_TYPE, value)

    def test_bytes_under_threshold_no_coercion(self, sync_connection: MagicMock) -> None:
        params = {"data": b"\x00" * 100}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_not_called()

    def test_bytes_over_threshold_becomes_blob(self, sync_connection: MagicMock) -> None:
        params = {"data": b"\x00" * 2001}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_called_once_with(BLOB_TYPE, b"\x00" * 2001)

    def test_bytearray_over_threshold_becomes_blob(self, sync_connection: MagicMock) -> None:
        params = {"data": bytearray(b"\x01" * 2001)}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_called_once()

    def test_mixed_parameters(self, sync_connection: MagicMock) -> None:
        params = {
            "small_str": "hello",
            "big_str": "x" * 5000,
            "small_bytes": b"\x00" * 100,
            "big_bytes": b"\xff" * 3000,
            "number": 42,
        }
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        assert params["small_str"] == "hello"
        assert params["big_str"] is sync_connection.createlob.return_value
        assert params["small_bytes"] == b"\x00" * 100
        assert params["number"] == 42
        assert sync_connection.createlob.call_count == 2

    def test_oracle_clob_wrapper_short_value_routed_to_clob(self, sync_connection: MagicMock) -> None:
        """OracleClob bypasses length threshold — explicit user intent."""
        from sqlspec.adapters.oracledb import OracleClob

        params = {"v": OracleClob("short text")}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_called_once_with(CLOB_TYPE, "short text")
        assert params["v"] is sync_connection.createlob.return_value

    def test_oracle_clob_wrapper_bytes_decoded_to_str(self, sync_connection: MagicMock) -> None:
        """OracleClob(bytes) is utf-8 decoded before createlob."""
        from sqlspec.adapters.oracledb import OracleClob

        params = {"v": OracleClob(b"some bytes")}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_called_once_with(CLOB_TYPE, "some bytes")

    def test_oracle_blob_wrapper_short_value_routed_to_blob(self, sync_connection: MagicMock) -> None:
        """OracleBlob bypasses length threshold — explicit user intent."""
        from sqlspec.adapters.oracledb import OracleBlob

        params = {"v": OracleBlob(b"short")}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_called_once_with(BLOB_TYPE, b"short")

    def test_oracle_blob_wrapper_str_encoded_to_bytes(self, sync_connection: MagicMock) -> None:
        """OracleBlob(str) is utf-8 encoded before createlob."""
        from sqlspec.adapters.oracledb import OracleBlob

        params = {"v": OracleBlob("text")}
        coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        sync_connection.createlob.assert_called_once_with(BLOB_TYPE, b"text")

    def test_oracle_json_wrapper_unwrapped_to_value(self, sync_connection: MagicMock) -> None:
        """OracleJson unwraps so the C1 input handler can claim the value."""
        from sqlspec.adapters.oracledb import OracleJson

        params = {"v": OracleJson({"a": 1})}
        result = coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        assert result["v"] == {"a": 1}
        sync_connection.createlob.assert_not_called()

    def test_threshold_override_keeps_5000_byte_str_as_varchar2(self, sync_connection: MagicMock) -> None:
        """varchar2_byte_limit=32767 (EXTENDED mode) keeps a 5000-byte str as VARCHAR2."""
        long_str = "x" * 5000
        params = {"v": long_str}
        result = coerce_large_parameters_sync(
            sync_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=32767,
            raw_byte_limit=RAW_LIMIT,
        )
        assert result["v"] is long_str
        sync_connection.createlob.assert_not_called()


# --- Async Tests ---


class TestCoerceLargeParametersAsync:
    @pytest.mark.anyio
    async def test_none_parameters_passthrough(self, async_connection: AsyncMock) -> None:
        result = await coerce_large_parameters_async(
            async_connection,
            None,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        assert result is None

    @pytest.mark.anyio
    async def test_string_over_threshold_becomes_clob(self, async_connection: AsyncMock) -> None:
        params = {"content": "a" * 4001}
        await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        async_connection.createlob.assert_called_once_with(CLOB_TYPE, "a" * 4001)

    @pytest.mark.anyio
    async def test_bytes_over_threshold_becomes_blob(self, async_connection: AsyncMock) -> None:
        params = {"data": b"\x00" * 2001}
        await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        async_connection.createlob.assert_called_once_with(BLOB_TYPE, b"\x00" * 2001)

    @pytest.mark.anyio
    async def test_multibyte_string_byte_threshold(self, async_connection: AsyncMock) -> None:
        value = "\u4e00" * 2000  # 6000 bytes
        params = {"content": value}
        await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        async_connection.createlob.assert_called_once_with(CLOB_TYPE, value)

    @pytest.mark.anyio
    async def test_oracle_clob_wrapper_short_value_routed_to_clob(self, async_connection: AsyncMock) -> None:
        """OracleClob bypasses length threshold \u2014 explicit user intent."""
        from sqlspec.adapters.oracledb import OracleClob

        params = {"v": OracleClob("short text")}
        await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        async_connection.createlob.assert_called_once_with(CLOB_TYPE, "short text")

    @pytest.mark.anyio
    async def test_oracle_clob_wrapper_bytes_decoded_to_str(self, async_connection: AsyncMock) -> None:
        """OracleClob(bytes) is utf-8 decoded before createlob."""
        from sqlspec.adapters.oracledb import OracleClob

        params = {"v": OracleClob(b"some bytes")}
        await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        async_connection.createlob.assert_called_once_with(CLOB_TYPE, "some bytes")

    @pytest.mark.anyio
    async def test_oracle_blob_wrapper_short_value_routed_to_blob(self, async_connection: AsyncMock) -> None:
        """OracleBlob bypasses length threshold \u2014 explicit user intent."""
        from sqlspec.adapters.oracledb import OracleBlob

        params = {"v": OracleBlob(b"short")}
        await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        async_connection.createlob.assert_called_once_with(BLOB_TYPE, b"short")

    @pytest.mark.anyio
    async def test_oracle_blob_wrapper_str_encoded_to_bytes(self, async_connection: AsyncMock) -> None:
        """OracleBlob(str) is utf-8 encoded before createlob."""
        from sqlspec.adapters.oracledb import OracleBlob

        params = {"v": OracleBlob("text")}
        await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        async_connection.createlob.assert_called_once_with(BLOB_TYPE, b"text")

    @pytest.mark.anyio
    async def test_oracle_json_wrapper_unwrapped_to_value(self, async_connection: AsyncMock) -> None:
        """OracleJson unwraps so the C1 input handler can claim the value."""
        from sqlspec.adapters.oracledb import OracleJson

        params = {"v": OracleJson({"a": 1})}
        result = await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=VARCHAR2_LIMIT,
            raw_byte_limit=RAW_LIMIT,
        )
        assert result["v"] == {"a": 1}
        async_connection.createlob.assert_not_called()

    @pytest.mark.anyio
    async def test_threshold_override_keeps_5000_byte_str_as_varchar2(self, async_connection: AsyncMock) -> None:
        """varchar2_byte_limit=32767 (EXTENDED mode) keeps a 5000-byte str as VARCHAR2."""
        long_str = "x" * 5000
        params = {"v": long_str}
        result = await coerce_large_parameters_async(
            async_connection,
            params,
            clob_type=CLOB_TYPE,
            blob_type=BLOB_TYPE,
            varchar2_byte_limit=32767,
            raw_byte_limit=RAW_LIMIT,
        )
        assert result["v"] is long_str
        async_connection.createlob.assert_not_called()
