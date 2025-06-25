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
-- name: collection-postgres-aws-extension-dependency
with proc_alias1 as (
  select distinct n.nspname as function_schema,
    p.proname as function_name,
    l.lanname as function_language,
    (
      select 'Y'
      from pg_trigger
      where tgfoid = (n.nspname || '.' || p.proname)::regproc
    ) as Trigger_Func,
    lower(pg_get_functiondef(p.oid)::text) as def
  from pg_proc p
    left join pg_namespace n on p.pronamespace = n.oid
    left join pg_language l on p.prolang = l.oid
    left join pg_type t on t.oid = p.prorettype
  where n.nspname not in (
      'pg_catalog',
      'information_schema',
      'aws_oracle_ext'
    )
    and p.prokind not in ('a', 'w', 'f')
    and l.lanname in ('sql', 'plpgsql')
  order by function_schema,
    function_name
),
proc_alias2 as (
  select proc_alias1.function_schema,
    proc_alias1.function_name,
    proc_alias1.function_language,
    proc_alias1.Trigger_Func,
    proc_alias2.*
  from proc_alias1
    cross join LATERAL (
      select i as funcname,
        cntgroup as cnt
      from (
          select (
              regexp_matches(
                proc_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1] i,
            count(1) cntgroup
          group by (
              regexp_matches(
                proc_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1]
        ) t
    ) as proc_alias2
  where def ~* 'aws_oracle_ext.*'
),
tbl_alias1 as (
  select alias1.proname,
    ns.nspname,
    case
      when relkind = 'r' then 'TABLE'
    end as objType,
    depend.relname,
    pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid) as def
  from pg_depend
    inner join (
      select distinct pg_proc.oid as procoid,
        nspname || '.' || proname as proname,
        pg_namespace.oid
      from pg_namespace,
        pg_proc
      where nspname = 'aws_oracle_ext'
        and pg_proc.pronamespace = pg_namespace.oid
    ) alias1 on pg_depend.refobjid = alias1.procoid
    inner join pg_attrdef on pg_attrdef.oid = pg_depend.objid
    inner join pg_class depend on depend.oid = pg_attrdef.adrelid
    inner join pg_namespace ns on ns.oid = depend.relnamespace
),
tbl_alias2 as (
  select tbl_alias1.nspname as SCHEMA,
    tbl_alias1.relname as TABLE_NAME,
    alias2.*
  from tbl_alias1
    cross join LATERAL (
      select i as funcname,
        cntgroup as cnt
      from (
          select (
              regexp_matches(
                tbl_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1] i,
            count(1) cntgroup
          group by (
              regexp_matches(
                tbl_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1]
        ) t
    ) as alias2
  where def ~* 'aws_oracle_ext.*'
),
constraint_alias1 as (
  select pgc.conname as CONSTRAINT_NAME,
    ccu.table_schema as table_schema,
    ccu.table_name,
    ccu.column_name,
    pg_get_constraintdef(pgc.oid) as def
  from pg_constraint pgc
    join pg_namespace nsp on nsp.oid = pgc.connamespace
    join pg_class cls on pgc.conrelid = cls.oid
    left join information_schema.constraint_column_usage ccu on pgc.conname = ccu.constraint_name
    and nsp.nspname = ccu.constraint_schema
  where contype = 'c'
  order by pgc.conname
),
constraint_alias2 as (
  select constraint_alias1.table_schema,
    constraint_alias1.constraint_name,
    constraint_alias1.table_name,
    constraint_alias1.column_name,
    alias2.*
  from constraint_alias1
    cross join LATERAL (
      select i as funcname,
        cntgroup as cnt
      from (
          select (
              regexp_matches(
                constraint_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1] i,
            count(1) cntgroup
          group by (
              regexp_matches(
                constraint_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1]
        ) t
    ) as alias2
  where def ~* 'aws_oracle_ext.*'
),
index_alias1 as (
  select alias1.proname,
    nspname,
    case
      when relkind = 'i' then 'INDEX'
    end as objType,
    depend.relname,
    pg_get_indexdef(depend.oid) def
  from pg_depend
    inner join (
      select distinct pg_proc.oid as procoid,
        nspname || '.' || proname as proname,
        pg_namespace.oid
      from pg_namespace,
        pg_proc
      where nspname = 'aws_oracle_ext'
        and pg_proc.pronamespace = pg_namespace.oid
    ) alias1 on pg_depend.refobjid = alias1.procoid
    inner join pg_class depend on depend.oid = pg_depend.objid
    inner join pg_namespace ns on ns.oid = depend.relnamespace
  where relkind = 'i'
),
index_alias2 as (
  select index_alias1.nspname as SCHEMA,
    index_alias1.relname as IndexName,
    alias2.*
  from index_alias1
    cross join LATERAL (
      select i as funcname,
        cntgroup as cnt
      from (
          select (
              regexp_matches(
                index_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1] i,
            count(1) cntgroup
          group by (
              regexp_matches(
                index_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1]
        ) t
    ) as alias2
  where def ~* 'aws_oracle_ext.*'
),
view_alias1 as (
  select alias1.proname,
    nspname,
    case
      when depend.relkind = 'v' then 'VIEW'
    end as objType,
    depend.relname,
    pg_get_viewdef(depend.oid) def
  from pg_depend
    inner join (
      select distinct pg_proc.oid as procoid,
        nspname || '.' || proname as proname,
        pg_namespace.oid
      from pg_namespace,
        pg_proc
      where nspname = 'aws_oracle_ext'
        and pg_proc.pronamespace = pg_namespace.oid
    ) alias1 on pg_depend.refobjid = alias1.procoid
    inner join pg_rewrite on pg_rewrite.oid = pg_depend.objid
    inner join pg_class depend on depend.oid = pg_rewrite.ev_class
    inner join pg_namespace ns on ns.oid = depend.relnamespace
  where not exists (
      select 1
      from pg_namespace
      where pg_namespace.oid = depend.relnamespace
        and nspname = 'aws_oracle_ext'
    )
),
view_alias2 as (
  select view_alias1.nspname as SCHEMA,
    view_alias1.relname as ViewName,
    alias2.*
  from view_alias1
    cross join LATERAL (
      select i as funcname,
        cntgroup as cnt
      from (
          select (
              regexp_matches(
                view_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1] i,
            count(1) cntgroup
          group by (
              regexp_matches(
                view_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1]
        ) t
    ) as alias2
  where def ~* 'aws_oracle_ext.*'
),
trigger_alias1 as (
  select distinct n.nspname as function_schema,
    p.proname as function_name,
    l.lanname as function_language,
    (
      select 'Y'
      from pg_trigger
      where tgfoid = (n.nspname || '.' || p.proname)::regproc
    ) as Trigger_Func,
    lower(pg_get_functiondef(p.oid)::text) as def
  from pg_proc p
    left join pg_namespace n on p.pronamespace = n.oid
    left join pg_language l on p.prolang = l.oid
    left join pg_type t on t.oid = p.prorettype
  where n.nspname not in (
      'pg_catalog',
      'information_schema',
      'aws_oracle_ext'
    )
    and p.prokind not in ('a', 'w')
    and l.lanname in ('sql', 'plpgsql')
  order by function_schema,
    function_name
),
trigger_alias2 as (
  select trigger_alias1.function_schema,
    trigger_alias1.function_name,
    trigger_alias1.function_language,
    trigger_alias1.Trigger_Func,
    alias2.*
  from trigger_alias1
    cross join LATERAL (
      select i as funcname,
        cntgroup as cnt
      from (
          select (
              regexp_matches(
                trigger_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1] i,
            count(1) cntgroup
          group by (
              regexp_matches(
                trigger_alias1.def,
                'aws_oracle_ext[.][a-z]*[_,a-z,$,"]*',
                'ig'
              )
            ) [1]
        ) t
    ) as alias2
  where def ~* 'aws_oracle_ext.*'
    and Trigger_Func = 'Y'
),
src as (
  select tbl_alias2.schema as schemaName,
    'N/A' as LANGUAGE,
    'TableDefaultConstraints' as type,
    tbl_alias2.table_name as typeName,
    tbl_alias2.funcname as AWSExtensionDependency,
    sum(cnt) as SCTFunctionReferenceCount
  from tbl_alias2
  group by tbl_alias2.schema,
    tbl_alias2.table_name,
    tbl_alias2.funcname
  union
  select function_schema as object_schema_name,
    function_language as object_language,
    'Procedures' as object_type,
    function_name as object_name,
    funcname as aws_extension_dependency,
    sum(cnt) as sct_function_reference_count
  from proc_alias2
  where 1 = 1
  group by function_schema,
    function_language,
    function_name,
    funcname
  union
  select constraint_alias2.table_schema as schemaName,
    'N/A' as LANGUAGE,
    'TableCheckConstraints' as type,
    constraint_alias2.table_name as typeName,
    constraint_alias2.funcname as AWSExtensionDependency,
    sum(cnt) as SCTFunctionReferenceCount
  from constraint_alias2
  group by constraint_alias2.table_schema,
    constraint_alias2.table_name,
    constraint_alias2.funcname
  union
  select index_alias2.Schema as schemaName,
    'N/A' as LANGUAGE,
    'TableIndexesAsFunctions' as type,
    index_alias2.IndexName as typeName,
    index_alias2.funcname as AWSExtensionDependency,
    sum(cnt) as SCTFunctionReferenceCount
  from index_alias2
  group by index_alias2.Schema,
    index_alias2.IndexName,
    index_alias2.funcname
  union
  select view_alias2.Schema as schemaName,
    'N/A' as LANGUAGE,
    'Views' as type,
    view_alias2.ViewName as typeName,
    view_alias2.funcname as AWSExtensionDependency,
    sum(cnt) as SCTFunctionReferenceCount
  from view_alias2
  group by view_alias2.Schema,
    view_alias2.ViewName,
    view_alias2.funcname
  union
  select function_schema as schemaName,
    function_language as LANGUAGE,
    'Triggers' as type,
    function_name as typeName,
    funcname as AWSExtensionDependency,
    sum(cnt) as SCTFunctionReferenceCount
  from trigger_alias2
  where 1 = 1
  group by function_schema,
    function_language,
    function_name,
    funcname
)
select :PKEY as pkey,
  :DMA_SOURCE_ID as dma_source_id,
  :DMA_MANUAL_ID as dma_manual_id,
  src.schemaName as schema_name,
  src.LANGUAGE as object_language,
  src.type as object_type,
  src.typeName as object_name,
  src.AWSExtensionDependency as aws_extension_dependency,
  src.SCTFunctionReferenceCount as sct_function_reference_count
from src;
