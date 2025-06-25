/*
 Copyright 2024 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
-- name: collection-postgres-base-replication-slots
with src as (
  select s.slot_name,
    s.plugin,
    s.slot_type,
    s.datoid,
    s.database,
    s.temporary,
    s.active,
    s.active_pid,
    s.xmin,
    s.catalog_xmin,
    s.restart_lsn,
    s.confirmed_flush_lsn,
    s.wal_status,
    s.safe_wal_size,
    s.two_phase
  from pg_replication_slots s
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.slot_name,
  src.plugin,
  src.slot_type,
  src.datoid,
  src.database,
  src.temporary,
  src.active,
  src.active_pid,
  src.xmin,
  src.catalog_xmin,
  src.restart_lsn,
  src.confirmed_flush_lsn,
  src.wal_status,
  src.safe_wal_size,
  src.two_phase
from src;

-- name: collection-postgres-12-replication-slots
with src as (
  select s.slot_name,
    s.plugin,
    s.slot_type,
    s.datoid,
    s.database,
    s.temporary,
    s.active,
    s.active_pid,
    s.xmin,
    s.catalog_xmin,
    s.restart_lsn,
    s.confirmed_flush_lsn --,
    --        s.wal_status,
    --        s.safe_wal_size --,
    --        s.two_phase
  from pg_replication_slots s
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.slot_name,
  src.plugin,
  src.slot_type,
  src.datoid,
  src.database,
  src.temporary,
  src.active,
  src.active_pid,
  src.xmin,
  src.catalog_xmin,
  src.restart_lsn,
  src.confirmed_flush_lsn,
  ' ' as wal_status,
  ' ' as safe_wal_size,
  ' ' as two_phase
from src;

-- name: collection-postgres-13-replication-slots
with src as (
  select s.slot_name,
    s.plugin,
    s.slot_type,
    s.datoid,
    s.database,
    s.temporary,
    s.active,
    s.active_pid,
    s.xmin,
    s.catalog_xmin,
    s.restart_lsn,
    s.confirmed_flush_lsn,
    s.wal_status,
    s.safe_wal_size --,
    --        s.two_phase
  from pg_replication_slots s
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.slot_name,
  src.plugin,
  src.slot_type,
  src.datoid,
  src.database,
  src.temporary,
  src.active,
  src.active_pid,
  src.xmin,
  src.catalog_xmin,
  src.restart_lsn,
  src.confirmed_flush_lsn,
  src.wal_status,
  src.safe_wal_size,
  ' ' as two_phase
from src;
