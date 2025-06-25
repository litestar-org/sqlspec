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
-- name: collection-mysql-users
select @PKEY as pkey,
  @DMA_SOURCE_ID as dma_source_id,
  @DMA_MANUAL_ID as dma_manual_id,
  user_host as user_host,
  user_count as user_count
from (
    select u.host as user_host,
      count(1) as user_count,
      sum(if(u.authentication_string = '', 1, 0)) as user_no_authentication_string_count,
      sum(
        if(
          u.host = '%'
          or u.host = '',
          1,
          0
        )
      ) as user_no_host_count,
      sum(
        if(
          u.authentication_string = '',
          1,
          0
        )
      ) as user_no_password_count,
      sum(
        if(
          u.shutdown_priv = 'Y'
          or u.super_priv = 'Y'
          or u.reload_priv = 'Y',
          1,
          0
        )
      ) as user_with_shutdown_privs_count
    from mysql.user u
    group by HOST
  ) src;
