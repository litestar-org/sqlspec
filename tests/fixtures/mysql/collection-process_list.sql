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
-- name: collection-mysql-base-process-list
select @PKEY as pkey,
  @DMA_SOURCE_ID as dma_source_id,
  @DMA_MANUAL_ID as dma_manual_id,
  id as process_id,
  HOST as process_host,
  db as process_db,
  command as process_command,
  TIME as process_time,
  state as process_state
from performance_schema.processlist;

-- name: collection-mysql-5-process-list
select @PKEY as pkey,
  @DMA_SOURCE_ID as dma_source_id,
  @DMA_MANUAL_ID as dma_manual_id,
  id as process_id,
  HOST as process_host,
  db as process_db,
  command as process_command,
  TIME as process_time,
  state as process_state
from information_schema.processlist;
