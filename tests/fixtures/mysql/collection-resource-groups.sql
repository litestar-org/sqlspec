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
-- name: collection-mysql-base-resource-groups
select concat(char(34), @PKEY, char(34)) as pkey,
  concat(char(34), @DMA_SOURCE_ID, char(34)) as dma_source_id,
  concat(char(34), @DMA_MANUAL_ID, char(34)) as dma_manual_id,
  concat(char(34), src.resource_group_name, char(34)) as resource_group_name,
  concat(char(34), src.resource_group_type, char(34)) as resource_group_type,
  concat(char(34), src.resource_group_enabled, char(34)) as resource_group_enabled,
  concat(char(34), src.vcpu_ids, char(34)) as vcpu_ids,
  concat(char(34), src.thread_priority, char(34)) as thread_priority
from (
    select rg.resource_group_type as resource_group_type,
      rg.resource_group_enabled as resource_group_enabled,
      rg.resource_group_name as resource_group_name,
      rg.vcpu_ids as vcpu_ids,
      rg.thread_priority as thread_priority
    from information_schema.resource_groups rg
  ) src;

-- name: collection-mysql-5-resource-groups
select concat(char(34), @PKEY, char(34)) as pkey,
  concat(char(34), @DMA_SOURCE_ID, char(34)) as dma_source_id,
  concat(char(34), @DMA_MANUAL_ID, char(34)) as dma_manual_id,
  concat(char(34), src.resource_group_name, char(34)) as resource_group_name,
  concat(char(34), src.resource_group_type, char(34)) as resource_group_type,
  concat(char(34), src.resource_group_enabled, char(34)) as resource_group_enabled,
  concat(char(34), src.vcpu_ids, char(34)) as vcpu_ids,
  concat(char(34), src.thread_priority, char(34)) as thread_priority
from (
    select 'Unsupported Version Placeholder' as resource_group_type,
      0 as resource_group_enabled,
      'Placeholder Value' as resource_group_name,
      '' as vcpu_ids,
      0 as thread_priority
    limit 0
  ) src;
