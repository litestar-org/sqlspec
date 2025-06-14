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
-- name: collection-mysql-engines
select @PKEY as pkey,
  @DMA_SOURCE_ID as dma_source_id,
  @DMA_MANUAL_ID as dma_manual_id,
  src.engine_name as engine_name,
  src.engine_support as engine_support,
  src.engine_transactions as engine_transactions,
  src.engine_xa as engine_xa,
  src.engine_savepoints as engine_savepoints,
  src.engine_comment as engine_comment
from (
    select i.ENGINE as engine_name,
      i.SUPPORT as engine_support,
      i.TRANSACTIONS as engine_transactions,
      i.xa as engine_xa,
      i.SAVEPOINTS as engine_savepoints,
      i.COMMENT as engine_comment
    from information_schema.ENGINES i
  ) src;
