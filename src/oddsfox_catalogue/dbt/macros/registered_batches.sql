{% macro registered_batches_checkpoint(relation=none) -%}
    {% set target = relation if relation is not none else this %}
    {{ return(target.incorporate(path={'identifier': '__checkpoint_' ~ target.identifier})) }}
{%- endmacro %}

{% macro registered_batches_pending() -%}
    {{ return(adapter.quote('__pending_' ~ this.schema ~ '_' ~ this.identifier)) }}
{%- endmacro %}

{% macro registered_batches_guard(checkpoint, version) -%}
    {% if execute %}
        {% set catalog = checkpoint.database | replace("'", "''") %}
        {% set schema = checkpoint.schema | replace("'", "''") %}
        {% set name = checkpoint.identifier | replace("'", "''") %}
        {% set shape = run_query("select column_name, data_type, is_nullable from information_schema.columns where table_catalog = '" ~ catalog ~ "' and table_schema = '" ~ schema ~ "' and table_name = '" ~ name ~ "' order by ordinal_position") %}
        {% set actual = [] %}
        {% for row in shape.rows %}
            {% do actual.append([row[0], row[1], row[2]]) %}
        {% endfor %}
        {% set kind = run_query("select table_type from information_schema.tables where table_catalog = '" ~ catalog ~ "' and table_schema = '" ~ schema ~ "' and table_name = '" ~ name ~ "'") %}
        {% if actual != [['record_kind', 'VARCHAR', 'NO'], ['batch_id', 'VARCHAR', 'NO'], ['processing_version', 'VARCHAR', 'NO']] or kind.rows | length != 1 or kind.rows[0][0] != 'BASE TABLE' %}
            {{ exceptions.raise_compiler_error('Invalid catalogue checkpoint schema; run --full-refresh --select ' ~ this.identifier) }}
        {% endif %}
    {% endif %}
select case when
    (select count(*) from {{ checkpoint }} where record_kind = 'metadata'
        and batch_id = '' and processing_version = '{{ version }}') = 1
    and not exists (select 1 from {{ checkpoint }} where
        processing_version is distinct from '{{ version }}' or record_kind is null or batch_id is null
        or record_kind not in ('metadata', 'batch')
        or (record_kind = 'metadata' and batch_id <> '')
        or (record_kind = 'batch' and batch_id = ''))
    and not exists (select record_kind, batch_id from {{ checkpoint }}
        group by record_kind, batch_id having count(*) <> 1)
    and not exists (select batch_id from {{ checkpoint }} where record_kind = 'batch'
        except select batch_id from {{ source('bronze', 'batch_registry') }} where status = 'loaded')
then 1 else error('Invalid catalogue checkpoint; run --full-refresh --select {{ this.identifier }}') end;
{%- endmacro %}

{% macro registered_batches_begin(version='1', dependencies=[]) -%}
    {% set checkpoint = registered_batches_checkpoint() %}
    {% set existing = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) if execute else none %}
    {% set progress = adapter.get_relation(database=this.database, schema=this.schema, identifier='__checkpoint_' ~ this.identifier) if execute else none %}
    {% set reset = existing is none or should_full_refresh() %}
    {% if not reset and existing.type != 'table' %}
        {{ exceptions.raise_compiler_error('Invalid catalogue incremental target; run --full-refresh --select ' ~ this.identifier) }}
    {% endif %}
    {% if not reset and (progress is none or progress.type != 'table') %}
        {{ exceptions.raise_compiler_error('Missing catalogue checkpoint; run --full-refresh --select ' ~ this.identifier) }}
    {% endif %}
    {% if reset %}
        {% if progress is not none and progress.type == 'view' %}
            drop view {{ checkpoint }};
        {% else %}
            drop table if exists {{ checkpoint }};
        {% endif %}
        create table {{ checkpoint }} (
            record_kind varchar not null,
            batch_id varchar not null,
            processing_version varchar not null,
            primary key (record_kind, batch_id)
        );
        insert into {{ checkpoint }} values ('metadata', '', '{{ version }}');
    {% else %}
        {{ registered_batches_guard(checkpoint, version) }}
    {% endif %}
    create or replace temp table {{ registered_batches_pending() }} as
        select batch_id from {{ source('bronze', 'batch_registry') }} where status = 'loaded'
        except select batch_id from {{ checkpoint }} where record_kind = 'batch';
    {% for dependency in dependencies %}
        {% set parent = ref(dependency) %}
        {% set parent_progress = registered_batches_checkpoint(parent) %}
        {{ registered_batches_guard(parent_progress, version) }}
        select case when not exists (
            select batch_id from {{ source('bronze', 'batch_registry') }} where status = 'loaded'
            except select batch_id from {{ parent_progress }} where record_kind = 'batch'
        ) then 1 else error('Stale catalogue dependency {{ dependency }}; build its ancestors before {{ this.identifier }}') end;
    {% endfor %}
{%- endmacro %}

{% macro registered_batches_finish(version='1') -%}
insert into {{ registered_batches_checkpoint() }}
    select 'batch', batch_id, '{{ version }}' from {{ registered_batches_pending() }}
    on conflict (record_kind, batch_id) do nothing;
{%- endmacro %}

{% macro registered_batches_predicate(column) -%}
    {{ column }} in (select batch_id from {{ registered_batches_pending() }})
{%- endmacro %}
