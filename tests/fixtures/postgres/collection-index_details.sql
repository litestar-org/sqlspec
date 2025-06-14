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
-- name: collection-postgres-index-details
with src as (
  select i.indexrelid as object_id,
    sut.relname as table_name,
    sut.schemaname as table_owner,
    ipc.relname as index_name,
    psui.schemaname as index_owner,
    i.indrelid as table_object_id,
    i.indnatts as indexed_column_count,
    i.indnkeyatts as indexed_keyed_column_count,
    i.indisunique as is_unique,
    i.indisprimary as is_primary,
    i.indisexclusion as is_exclusion,
    i.indimmediate as is_immediate,
    i.indisclustered as is_clustered,
    i.indisvalid as is_valid,
    i.indcheckxmin as is_check_xmin,
    i.indisready as is_ready,
    i.indislive as is_live,
    i.indisreplident as is_replica_identity,
    psui.idx_blks_read as index_block_read,
    psui.idx_blks_hit as index_blocks_hit,
    p.idx_scan as index_scan,
    p.idx_tup_read as index_tuples_read,
    p.idx_tup_fetch as index_tuples_fetched
  from pg_index i
    join pg_stat_user_tables sut on (i.indrelid = sut.relid)
    join pg_class ipc on (i.indexrelid = ipc.oid)
    left join pg_catalog.pg_statio_user_indexes psui on (i.indexrelid = psui.indexrelid)
    left join pg_catalog.pg_stat_user_indexes p on (i.indexrelid = p.indexrelid)
  where psui.indexrelid is not null
    or p.indexrelid is not null
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.object_id,
  replace(src.table_name, chr(34), chr(30)) as table_name,
  replace(src.table_owner, chr(34), chr(30)) as table_owner,
  replace(src.index_name, chr(34), chr(30)) as index_name,
  replace(src.index_owner, chr(34), chr(30)) as index_owner,
  src.table_object_id,
  src.indexed_column_count,
  src.indexed_keyed_column_count,
  src.is_unique,
  src.is_primary,
  src.is_exclusion,
  src.is_immediate,
  src.is_clustered,
  src.is_valid,
  src.is_check_xmin,
  src.is_ready,
  src.is_live,
  src.is_replica_identity,
  src.index_block_read,
  src.index_blocks_hit,
  src.index_scan,
  src.index_tuples_read,
  src.index_tuples_fetched
from src;
