"""Compiled orchestration for staged bulk loading and multi-shard storage ingestion."""

import asyncio
import contextlib
import multiprocessing
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, cast

from sqlspec.data_dictionary._index_ddl import (
    AsyncWithoutIndexesContext,
    SyncWithoutIndexesContext,
    ensure_teradata_ddl_outside_transaction,
)
from sqlspec.exceptions import SQLSpecError
from sqlspec.storage._arrow_payload import (
    decode_arrow_payload,
    decode_shard_in_process_worker,
    init_spawn_worker_thread_budget,
    read_shard_bytes_and_decode,
)
from sqlspec.storage._staging_sql import (
    SAP_DIALECTS,
    build_checkpoint_insert_sql,
    build_checkpoint_select_sql,
    build_checkpoint_table_ddl,
    build_drop_stage_table_sql,
    build_stage_merge_sql,
    build_stage_table_ddl,
    build_teradata_checkpoint_probe,
    format_storage_identifier,
    normalize_storage_dialect,
    resolve_stage_table_name,
)
from sqlspec.storage.pipeline import (
    StorageBridgeJob,
    StorageDestination,
    StorageFormat,
    StorageTelemetry,
    attach_partition_telemetry,
    create_storage_job,
    ingest_telemetry_from_batches,
)
from sqlspec.utils.arrow_helpers import coerce_arrow_table

__all__ = (
    "execute_load_from_storage_async",
    "execute_load_from_storage_sync",
    "execute_staged_load_async",
    "execute_staged_load_sync",
)


def execute_staged_load_sync(
    driver: Any,
    table: str,
    source: Any,
    *,
    raw_loader: Callable[..., StorageBridgeJob],
    partitioner: dict[str, object] | None = None,
    overwrite: bool = False,
    telemetry: StorageTelemetry | None = None,
    stage_table: bool | str | None = None,
    conflict_keys: Sequence[str] | None = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: Sequence[str] | None = None,
    drop_stage_table: bool = True,
    drop_indexes: bool = False,
    concurrent_indexes: bool = False,
    type_casts: Mapping[str, str] | None = None,
    multi_worker: bool = False,
) -> StorageBridgeJob:
    """Orchestrate index drop/rebuild and staging table creation/merge/cleanup for sync bulk loads."""
    if drop_indexes or stage_table or conflict_keys:
        ensure_teradata_ddl_outside_transaction(driver, "staged load")
    if drop_indexes:
        with SyncWithoutIndexesContext(driver, table, concurrent=concurrent_indexes):
            return execute_staged_load_sync(
                driver,
                table,
                source,
                raw_loader=raw_loader,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
                multi_worker=multi_worker,
            )

    dialect = normalize_storage_dialect(str(getattr(driver, "dialect", "") or ""))
    resolved_stage = resolve_stage_table_name(table, stage_table, conflict_keys=conflict_keys, dialect=dialect)
    if resolved_stage is None:
        return raw_loader(table, source, partitioner=partitioner, overwrite=overwrite, telemetry=telemetry)

    arrow_table = coerce_arrow_table(source)
    columns = list(arrow_table.column_names)
    execute_ddl = driver.execute_script if dialect in SAP_DIALECTS else driver.execute
    for ddl in build_stage_table_ddl(dialect, table, resolved_stage, multi_worker=multi_worker):
        execute_ddl(ddl)
    try:
        if overwrite and not conflict_keys:
            driver.execute(f"DELETE FROM {format_storage_identifier(table, dialect=dialect, allow_qualified=True)}")
        job = raw_loader(resolved_stage, arrow_table, partitioner=partitioner, overwrite=False, telemetry=telemetry)
        if columns and arrow_table.num_rows > 0:
            _validate_stage_conflict_keys(columns, conflict_keys)
            merge_sql = build_stage_merge_sql(
                dialect,
                table,
                resolved_stage,
                columns,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                type_casts=type_casts,
            )
            if dialect in {"sybase", "teradata"} and conflict_keys:
                driver.execute_script(merge_sql)
            else:
                driver.execute(merge_sql)
        job.telemetry["destination"] = table
        return job
    finally:
        if drop_stage_table:
            with contextlib.suppress(Exception):
                driver.execute(build_drop_stage_table_sql(dialect, resolved_stage))


async def execute_staged_load_async(
    driver: Any,
    table: str,
    source: Any,
    *,
    raw_loader: Callable[..., Any],
    partitioner: dict[str, object] | None = None,
    overwrite: bool = False,
    telemetry: StorageTelemetry | None = None,
    stage_table: bool | str | None = None,
    conflict_keys: Sequence[str] | None = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: Sequence[str] | None = None,
    drop_stage_table: bool = True,
    drop_indexes: bool = False,
    concurrent_indexes: bool = False,
    type_casts: Mapping[str, str] | None = None,
    multi_worker: bool = False,
) -> StorageBridgeJob:
    """Orchestrate index drop/rebuild and staging table creation/merge/cleanup for async bulk loads."""
    if drop_indexes or stage_table or conflict_keys:
        ensure_teradata_ddl_outside_transaction(driver, "staged load")
    if drop_indexes:
        async with AsyncWithoutIndexesContext(driver, table, concurrent=concurrent_indexes):
            return await execute_staged_load_async(
                driver,
                table,
                source,
                raw_loader=raw_loader,
                partitioner=partitioner,
                overwrite=overwrite,
                telemetry=telemetry,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                type_casts=type_casts,
                multi_worker=multi_worker,
            )

    dialect = normalize_storage_dialect(str(getattr(driver, "dialect", "") or ""))
    resolved_stage = resolve_stage_table_name(table, stage_table, conflict_keys=conflict_keys, dialect=dialect)
    if resolved_stage is None:
        return cast(
            "StorageBridgeJob",
            await raw_loader(table, source, partitioner=partitioner, overwrite=overwrite, telemetry=telemetry),
        )

    arrow_table = coerce_arrow_table(source)
    columns = list(arrow_table.column_names)
    execute_ddl = driver.execute_script if dialect in SAP_DIALECTS else driver.execute
    for ddl in build_stage_table_ddl(dialect, table, resolved_stage, multi_worker=multi_worker):
        await execute_ddl(ddl)
    try:
        if overwrite and not conflict_keys:
            await driver.execute(
                f"DELETE FROM {format_storage_identifier(table, dialect=dialect, allow_qualified=True)}"
            )
        job = cast(
            "StorageBridgeJob",
            await raw_loader(
                resolved_stage, arrow_table, partitioner=partitioner, overwrite=False, telemetry=telemetry
            ),
        )
        if columns and arrow_table.num_rows > 0:
            _validate_stage_conflict_keys(columns, conflict_keys)
            merge_sql = build_stage_merge_sql(
                dialect,
                table,
                resolved_stage,
                columns,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                type_casts=type_casts,
            )
            if dialect in {"sybase", "teradata"} and conflict_keys:
                await driver.execute_script(merge_sql)
            else:
                await driver.execute(merge_sql)
        job.telemetry["destination"] = table
        return job
    finally:
        if drop_stage_table:
            with contextlib.suppress(Exception):
                await driver.execute(build_drop_stage_table_sql(dialect, resolved_stage))


def execute_load_from_storage_sync(
    driver: Any,
    table: str,
    source: StorageDestination | Sequence[StorageDestination],
    *,
    file_format: StorageFormat,
    partitioner: dict[str, object] | None = None,
    overwrite: bool = False,
    storage_options: dict[str, Any] | None = None,
    stage_table: bool | str | None = None,
    conflict_keys: Sequence[str] | None = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: Sequence[str] | None = None,
    drop_stage_table: bool = True,
    drop_indexes: bool = False,
    concurrent_indexes: bool = False,
    checkpoint_table: str | None = None,
    checkpoint_phase: str = "default",
    max_workers: int = 1,
    executor: Literal["thread", "process"] = "thread",
    arrow_schema: Any = None,
    type_casts: Mapping[str, str] | None = None,
) -> StorageBridgeJob:
    """Execute single-shard or multi-shard synchronous storage ingestion with checkpointing, staging, and index management."""
    is_source_sequence, source_str, has_glob_or_prefix = _is_multi_shard_source(source)

    is_simple_single_load = (
        not has_glob_or_prefix
        and checkpoint_table is None
        and max_workers <= 1
        and stage_table is None
        and conflict_keys is None
        and not drop_indexes
        and arrow_schema is None
    )
    if is_simple_single_load:
        arrow_table, inbound = (
            driver._read_storage_arrow(source, file_format=file_format)
            if storage_options is None
            else driver._read_storage_arrow(source, file_format=file_format, storage_options=storage_options)
        )
        return cast(
            "StorageBridgeJob",
            driver.load_from_arrow(table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound),
        )

    if drop_indexes or checkpoint_table or stage_table or conflict_keys:
        ensure_teradata_ddl_outside_transaction(driver, "storage load")
    if drop_indexes:
        with SyncWithoutIndexesContext(driver, table, concurrent=concurrent_indexes):
            return execute_load_from_storage_sync(
                driver,
                table,
                source,
                file_format=file_format,
                partitioner=partitioner,
                overwrite=overwrite,
                storage_options=storage_options,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                checkpoint_table=checkpoint_table,
                checkpoint_phase=checkpoint_phase,
                max_workers=max_workers,
                executor=executor,
                arrow_schema=arrow_schema,
                type_casts=type_casts,
            )

    pipeline = driver._storage_pipeline()
    if is_source_sequence:
        shards = [
            item.as_posix() if isinstance(item, Path) else str(item)
            for item in cast("Sequence[StorageDestination]", source)
        ]
    elif has_glob_or_prefix:
        shards = pipeline.list_sources(source, file_format=file_format, storage_options=storage_options)
    else:
        shards = [source_str]
    dialect = normalize_storage_dialect(str(getattr(driver, "dialect", "") or ""))
    completed_uris: set[str] = set()
    if checkpoint_table:
        _ensure_checkpoint_table_sync(driver, dialect, checkpoint_table)
        res = driver.execute(
            build_checkpoint_select_sql(checkpoint_table, dialect=dialect), {"phase": checkpoint_phase}
        )
        for row in res.data or []:
            uri_val = row.get("shard_uri") if isinstance(row, Mapping) else row[0]
            if uri_val is not None:
                completed_uris.add(str(uri_val))

    pending_shards = [s for s in shards if s not in completed_uris]
    shards_skipped = len(shards) - len(pending_shards)

    resolved_stage = resolve_stage_table_name(table, stage_table, conflict_keys=conflict_keys, dialect=dialect)
    effective_target = resolved_stage or table
    if resolved_stage is not None:
        execute_ddl = driver.execute_script if dialect in SAP_DIALECTS else driver.execute
        for ddl in build_stage_table_ddl(dialect, table, resolved_stage, multi_worker=max_workers > 1):
            execute_ddl(ddl)

    total_rows = 0
    total_bytes = 0
    read_duration = 0.0
    write_duration = 0.0
    observed_columns: list[str] = []
    wrote_first_non_empty_shard = False
    start_wall = perf_counter()

    try:
        if overwrite and pending_shards and not conflict_keys:
            driver.execute(f"DELETE FROM {format_storage_identifier(table, dialect=dialect, allow_qualified=True)}")

        decoded_shards = _decode_pending_shards_sync(
            pending_shards,
            file_format=file_format,
            storage_options=storage_options,
            arrow_schema=arrow_schema,
            max_workers=max_workers,
            executor=executor,
        )

        for shard_uri, shard_table, shard_bytes, r_dur in decoded_shards:
            read_duration += r_dur
            total_bytes += int(shard_bytes)
            if int(getattr(shard_table, "num_rows", 0)) == 0:
                if checkpoint_table:
                    _record_checkpoint_sync(
                        driver, dialect, checkpoint_table, checkpoint_phase, shard_uri, 0, shard_bytes, r_dur
                    )
                continue
            shard_cols = list(shard_table.column_names) if hasattr(shard_table, "column_names") else []
            if not observed_columns:
                observed_columns = shard_cols
            elif shard_cols != observed_columns:
                msg = f"Shard {shard_uri!r} columns {shard_cols} do not match expected columns {observed_columns}"
                raise SQLSpecError(msg)
            w0 = perf_counter()
            shard_overwrite = bool(
                overwrite and not wrote_first_non_empty_shard and resolved_stage is None and not conflict_keys
            )
            wrote_first_non_empty_shard = True
            shard_job = driver.load_from_arrow(
                effective_target, shard_table, partitioner=partitioner, overwrite=shard_overwrite
            )
            w_dur = perf_counter() - w0
            write_duration += w_dur
            s_rows = int(shard_job.telemetry.get("rows_processed", shard_table.num_rows))
            total_rows += s_rows
            if checkpoint_table:
                _record_checkpoint_sync(
                    driver, dialect, checkpoint_table, checkpoint_phase, shard_uri, s_rows, shard_bytes, r_dur + w_dur
                )

        if resolved_stage is not None and observed_columns and total_rows > 0:
            _validate_stage_conflict_keys(observed_columns, conflict_keys)
            merge_sql = build_stage_merge_sql(
                dialect,
                table,
                resolved_stage,
                observed_columns,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                type_casts=type_casts,
            )
            w0 = perf_counter()
            if dialect in {"sybase", "teradata"} and conflict_keys:
                driver.execute_script(merge_sql)
            else:
                driver.execute(merge_sql)
            write_duration += perf_counter() - w0
    finally:
        if resolved_stage is not None and drop_stage_table:
            with contextlib.suppress(Exception):
                driver.execute(build_drop_stage_table_sql(dialect, resolved_stage))

    telemetry_payload = ingest_telemetry_from_batches(
        total_rows,
        total_bytes,
        duration_s=perf_counter() - start_wall,
        format_label=file_format,
        read_duration_s=read_duration,
        write_duration_s=write_duration,
        shards_processed=len(pending_shards),
        shards_skipped=shards_skipped,
    )
    telemetry_payload["destination"] = table
    attach_partition_telemetry(telemetry_payload, partitioner)
    return create_storage_job(telemetry_payload)


async def execute_load_from_storage_async(
    driver: Any,
    table: str,
    source: StorageDestination | Sequence[StorageDestination],
    *,
    file_format: StorageFormat,
    partitioner: dict[str, object] | None = None,
    overwrite: bool = False,
    storage_options: dict[str, Any] | None = None,
    stage_table: bool | str | None = None,
    conflict_keys: Sequence[str] | None = None,
    on_conflict: Literal["update", "nothing"] = "update",
    exclude_update_columns: Sequence[str] | None = None,
    drop_stage_table: bool = True,
    drop_indexes: bool = False,
    concurrent_indexes: bool = False,
    checkpoint_table: str | None = None,
    checkpoint_phase: str = "default",
    max_workers: int = 1,
    executor: Literal["thread", "process"] = "thread",
    arrow_schema: Any = None,
    type_casts: Mapping[str, str] | None = None,
) -> StorageBridgeJob:
    """Execute single-shard or multi-shard asynchronous storage ingestion with checkpointing, staging, and index management."""
    _ = executor
    is_source_sequence, source_str, has_glob_or_prefix = _is_multi_shard_source(source)

    is_simple_single_load = (
        not has_glob_or_prefix
        and checkpoint_table is None
        and max_workers <= 1
        and stage_table is None
        and conflict_keys is None
        and not drop_indexes
        and arrow_schema is None
    )
    if is_simple_single_load:
        arrow_table, inbound = (
            await driver._read_storage_arrow(source, file_format=file_format)
            if storage_options is None
            else await driver._read_storage_arrow(source, file_format=file_format, storage_options=storage_options)
        )
        return cast(
            "StorageBridgeJob",
            await driver.load_from_arrow(
                table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
            ),
        )

    if drop_indexes or checkpoint_table or stage_table or conflict_keys:
        ensure_teradata_ddl_outside_transaction(driver, "storage load")
    if drop_indexes:
        async with AsyncWithoutIndexesContext(driver, table, concurrent=concurrent_indexes):
            return await execute_load_from_storage_async(
                driver,
                table,
                source,
                file_format=file_format,
                partitioner=partitioner,
                overwrite=overwrite,
                storage_options=storage_options,
                stage_table=stage_table,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                drop_stage_table=drop_stage_table,
                drop_indexes=False,
                concurrent_indexes=concurrent_indexes,
                checkpoint_table=checkpoint_table,
                checkpoint_phase=checkpoint_phase,
                max_workers=max_workers,
                executor=executor,
                arrow_schema=arrow_schema,
                type_casts=type_casts,
            )

    pipeline = driver._storage_pipeline()
    if is_source_sequence:
        shards = [
            item.as_posix() if isinstance(item, Path) else str(item)
            for item in cast("Sequence[StorageDestination]", source)
        ]
    elif has_glob_or_prefix:
        shards = await pipeline.list_sources_async(source, file_format=file_format, storage_options=storage_options)
    else:
        shards = [source_str]
    dialect = normalize_storage_dialect(str(getattr(driver, "dialect", "") or ""))
    completed_uris: set[str] = set()
    if checkpoint_table:
        await _ensure_checkpoint_table_async(driver, dialect, checkpoint_table)
        res = await driver.execute(
            build_checkpoint_select_sql(checkpoint_table, dialect=dialect), {"phase": checkpoint_phase}
        )
        for row in res.data or []:
            uri_val = row.get("shard_uri") if isinstance(row, Mapping) else row[0]
            if uri_val is not None:
                completed_uris.add(str(uri_val))

    pending_shards = [s for s in shards if s not in completed_uris]
    shards_skipped = len(shards) - len(pending_shards)

    resolved_stage = resolve_stage_table_name(table, stage_table, conflict_keys=conflict_keys, dialect=dialect)
    effective_target = resolved_stage or table
    if resolved_stage is not None:
        execute_ddl = driver.execute_script if dialect in SAP_DIALECTS else driver.execute
        for ddl in build_stage_table_ddl(dialect, table, resolved_stage, multi_worker=max_workers > 1):
            await execute_ddl(ddl)

    total_rows = 0
    total_bytes = 0
    read_duration = 0.0
    write_duration = 0.0
    observed_columns: list[str] = []
    wrote_first_non_empty_shard = False
    start_wall = perf_counter()

    try:
        if overwrite and pending_shards and not conflict_keys:
            await driver.execute(
                f"DELETE FROM {format_storage_identifier(table, dialect=dialect, allow_qualified=True)}"
            )

        decoded_shards: list[tuple[str, Any, int, float]] = []
        if pending_shards:
            loop = asyncio.get_running_loop()
            if max_workers > 1 and len(pending_shards) > 1:
                with ThreadPoolExecutor(max_workers=max_workers) as pool:
                    tasks = [
                        loop.run_in_executor(
                            pool, read_shard_bytes_and_decode, s, file_format, storage_options, arrow_schema
                        )
                        for s in pending_shards
                    ]
                    decoded_shards = list(await asyncio.gather(*tasks))
            else:
                for shard_uri in pending_shards:
                    decoded_shards.append(
                        await loop.run_in_executor(
                            None, read_shard_bytes_and_decode, shard_uri, file_format, storage_options, arrow_schema
                        )
                    )

        for shard_uri, shard_table, shard_bytes, r_dur in decoded_shards:
            read_duration += r_dur
            total_bytes += int(shard_bytes)
            if int(getattr(shard_table, "num_rows", 0)) == 0:
                if checkpoint_table:
                    await _record_checkpoint_async(
                        driver, dialect, checkpoint_table, checkpoint_phase, shard_uri, 0, shard_bytes, r_dur
                    )
                continue
            shard_cols = list(shard_table.column_names) if hasattr(shard_table, "column_names") else []
            if not observed_columns:
                observed_columns = shard_cols
            elif shard_cols != observed_columns:
                msg = f"Shard {shard_uri!r} columns {shard_cols} do not match expected columns {observed_columns}"
                raise SQLSpecError(msg)
            w0 = perf_counter()
            shard_overwrite = bool(
                overwrite and not wrote_first_non_empty_shard and resolved_stage is None and not conflict_keys
            )
            wrote_first_non_empty_shard = True
            shard_job = await driver.load_from_arrow(
                effective_target, shard_table, partitioner=partitioner, overwrite=shard_overwrite
            )
            w_dur = perf_counter() - w0
            write_duration += w_dur
            s_rows = int(shard_job.telemetry.get("rows_processed", shard_table.num_rows))
            total_rows += s_rows
            if checkpoint_table:
                await _record_checkpoint_async(
                    driver, dialect, checkpoint_table, checkpoint_phase, shard_uri, s_rows, shard_bytes, r_dur + w_dur
                )

        if resolved_stage is not None and observed_columns and total_rows > 0:
            _validate_stage_conflict_keys(observed_columns, conflict_keys)
            merge_sql = build_stage_merge_sql(
                dialect,
                table,
                resolved_stage,
                observed_columns,
                conflict_keys=conflict_keys,
                on_conflict=on_conflict,
                exclude_update_columns=exclude_update_columns,
                type_casts=type_casts,
            )
            w0 = perf_counter()
            if dialect in {"sybase", "teradata"} and conflict_keys:
                await driver.execute_script(merge_sql)
            else:
                await driver.execute(merge_sql)
            write_duration += perf_counter() - w0
    finally:
        if resolved_stage is not None and drop_stage_table:
            with contextlib.suppress(Exception):
                await driver.execute(build_drop_stage_table_sql(dialect, resolved_stage))

    telemetry_payload = ingest_telemetry_from_batches(
        total_rows,
        total_bytes,
        duration_s=perf_counter() - start_wall,
        format_label=file_format,
        read_duration_s=read_duration,
        write_duration_s=write_duration,
        shards_processed=len(pending_shards),
        shards_skipped=shards_skipped,
    )
    telemetry_payload["destination"] = table
    attach_partition_telemetry(telemetry_payload, partitioner)
    return create_storage_job(telemetry_payload)


def _ensure_checkpoint_table_sync(driver: Any, dialect: str, checkpoint_table: str) -> None:
    """Create checkpoint storage synchronously, tolerating an existing Teradata table."""
    dialect_key = normalize_storage_dialect(dialect)
    if dialect_key == "teradata":
        probe_sql, probe_params = build_teradata_checkpoint_probe(checkpoint_table)
        probe_result = driver.execute(probe_sql, probe_params)
        if getattr(probe_result, "data", None):
            return
    try:
        driver.execute(build_checkpoint_table_ddl(dialect_key, checkpoint_table))
    except Exception as exc:
        message = str(exc).lower()
        if dialect_key != "teradata" or ("[error 3803]" not in message and "already exists" not in message):
            raise


async def _ensure_checkpoint_table_async(driver: Any, dialect: str, checkpoint_table: str) -> None:
    """Create checkpoint storage asynchronously, tolerating an existing Teradata table."""
    dialect_key = normalize_storage_dialect(dialect)
    if dialect_key == "teradata":
        probe_sql, probe_params = build_teradata_checkpoint_probe(checkpoint_table)
        probe_result = await driver.execute(probe_sql, probe_params)
        if getattr(probe_result, "data", None):
            return
    try:
        await driver.execute(build_checkpoint_table_ddl(dialect_key, checkpoint_table))
    except Exception as exc:
        message = str(exc).lower()
        if dialect_key != "teradata" or ("[error 3803]" not in message and "already exists" not in message):
            raise


def _validate_stage_conflict_keys(columns: Sequence[str], conflict_keys: Sequence[str] | None) -> None:
    """Verify every requested conflict key column is present in the staged column set."""
    if not conflict_keys:
        return
    col_set = set(columns)
    missing = [key for key in conflict_keys if key not in col_set]
    if missing:
        msg = f"Conflict keys {missing} are not present in staged columns {list(columns)}"
        raise SQLSpecError(msg)


def _record_checkpoint_sync(
    driver: Any,
    dialect: str,
    checkpoint_table: str,
    phase: str,
    shard_uri: str,
    rows_loaded: int,
    bytes_processed: int,
    duration_s: float,
) -> None:
    sql_text = build_checkpoint_insert_sql(dialect, checkpoint_table)
    driver.execute(
        sql_text,
        {
            "phase": phase,
            "shard_uri": shard_uri,
            "rows_loaded": int(rows_loaded),
            "bytes_processed": int(bytes_processed),
            "duration_s": float(duration_s),
        },
    )


async def _record_checkpoint_async(
    driver: Any,
    dialect: str,
    checkpoint_table: str,
    phase: str,
    shard_uri: str,
    rows_loaded: int,
    bytes_processed: int,
    duration_s: float,
) -> None:
    sql_text = build_checkpoint_insert_sql(dialect, checkpoint_table)
    await driver.execute(
        sql_text,
        {
            "phase": phase,
            "shard_uri": shard_uri,
            "rows_loaded": int(rows_loaded),
            "bytes_processed": int(bytes_processed),
            "duration_s": float(duration_s),
        },
    )


def _decode_pending_shards_sync(
    pending_shards: Sequence[str],
    *,
    file_format: StorageFormat,
    storage_options: dict[str, Any] | None,
    arrow_schema: Any,
    max_workers: int,
    executor: Literal["thread", "process"],
) -> list[tuple[str, Any, int, float]]:
    """Decode pending shards synchronously using sequential, thread-pool, or spawn process-pool execution."""
    if not pending_shards:
        return []
    if max_workers > 1 and len(pending_shards) > 1:
        if executor == "process":
            mp_ctx = multiprocessing.get_context("spawn")
            decoded: list[tuple[str, Any, int, float]] = []
            with ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=mp_ctx,
                initializer=init_spawn_worker_thread_budget,
                initargs=(max_workers,),
            ) as pool:
                worker_args = [(s, file_format, storage_options) for s in pending_shards]
                for shard_uri, ipc_bytes, raw_bytes, r_dur in pool.map(decode_shard_in_process_worker, worker_args):
                    tbl = decode_arrow_payload(ipc_bytes, "arrow-ipc", arrow_schema=arrow_schema)
                    decoded.append((shard_uri, tbl, raw_bytes, r_dur))
            return decoded
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(read_shard_bytes_and_decode, s, file_format, storage_options, arrow_schema)
                for s in pending_shards
            ]
            return [f.result() for f in futures]
    return [
        read_shard_bytes_and_decode(shard_uri, file_format, storage_options, arrow_schema)
        for shard_uri in pending_shards
    ]


def _is_multi_shard_source(source: StorageDestination | Sequence[StorageDestination]) -> tuple[bool, str, bool]:
    """Return ``(is_source_sequence, source_str, has_glob_or_prefix)`` for a storage destination."""
    if isinstance(source, Sequence) and not isinstance(source, (str, bytes, bytearray)):
        return True, "", True
    source_str = source.as_posix() if isinstance(source, Path) else str(source)
    if any(ch in source_str for ch in ("*", "?", "[")) or source_str.endswith("/"):
        return False, source_str, True
    if "://" not in source_str:
        with contextlib.suppress(Exception):
            if Path(source_str).expanduser().is_dir():
                return False, source_str, True
    return False, source_str, False
