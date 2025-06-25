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
-- name: collection-postgres-replication-stats
with src as (
  select r.pid,
    r.usesysid,
    r.usename,
    r.application_name,
    host(r.client_addr) as client_addr,
    r.client_hostname,
    r.client_port,
    r.backend_start,
    r.backend_xmin,
    r.state,
    r.sent_lsn,
    r.write_lsn,
    r.flush_lsn,
    r.replay_lsn,
    r.write_lag,
    r.flush_lag,
    r.replay_lag,
    r.sync_priority,
    r.sync_state,
    r.reply_time
  from pg_stat_replication r
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.pid,
  src.usesysid,
  src.usename,
  src.application_name,
  src.client_addr,
  src.client_hostname,
  src.client_port,
  src.backend_start,
  src.backend_xmin,
  src.state,
  src.sent_lsn,
  src.write_lsn,
  src.flush_lsn,
  src.replay_lsn,
  src.write_lag,
  src.flush_lag,
  src.replay_lag,
  src.sync_priority,
  src.sync_state,
  src.reply_time
from src;
