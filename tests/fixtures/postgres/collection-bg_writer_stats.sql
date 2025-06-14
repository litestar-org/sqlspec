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
-- name: collection-postgres-bg-writer-stats
with src as (
  select w.checkpoints_timed,
    w.checkpoints_req as checkpoints_requested,
    w.checkpoint_write_time,
    w.checkpoint_sync_time,
    w.buffers_checkpoint,
    w.buffers_clean,
    w.maxwritten_clean as max_written_clean,
    w.buffers_backend,
    w.buffers_backend_fsync,
    w.buffers_alloc as buffers_allocated,
    w.stats_reset
  from pg_stat_bgwriter w
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.checkpoints_timed,
  src.checkpoints_requested,
  src.checkpoint_write_time,
  src.checkpoint_sync_time,
  src.buffers_checkpoint,
  src.buffers_clean,
  src.max_written_clean,
  src.buffers_backend,
  src.buffers_backend_fsync,
  src.buffers_allocated,
  src.stats_reset
from src;

-- name: collection-postgres-bg-writer-stats-from-pg17
with src as (
  select
    w.buffers_clean,
    w.maxwritten_clean as max_written_clean,
    w.buffers_alloc as buffers_allocated,
    w.stats_reset
  from pg_stat_bgwriter w
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.buffers_clean,
  src.max_written_clean,
  src.buffers_allocated,
  src.stats_reset
from src;
