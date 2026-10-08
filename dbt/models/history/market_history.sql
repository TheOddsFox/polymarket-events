-- SCD2 versions per market. Same contract as event_history.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['venue', 'market_id'],
    on_schema_change='fail'
) }}

with sem as (
    select * from {{ ref('int_market_semantic') }}
),

{% if is_incremental() %}
scope as (
    select distinct venue, market_id
    from sem
    where batch_loaded_at >= (
        select coalesce(max(built_through), timestamp '1970-01-01 00:00:00+00') from {{ this }}
    )
),

scoped as (
    select s.*
    from sem s
    join scope using (venue, market_id)
),
{% else %}
scoped as (
    select * from sem
),
{% endif %}

starts as (
    select
        *,
        case
            when semantic_hash is distinct from lag(semantic_hash) over (
                partition by venue, market_id order by observed_at, observation_id
            ) then 1
            else 0
        end as starts_version
    from scoped
),

numbered as (
    select
        *,
        sum(starts_version) over (
            partition by venue, market_id
            order by observed_at, observation_id
            rows between unbounded preceding and current row
        ) as version_no
    from starts
),

versions as (
    select
        venue,
        market_id,
        version_no,
        min(observed_at) as valid_from,
        max(observed_at) as last_observed_at,
        count(*) as observation_count,
        any_value(semantic_hash) as semantic_hash,
        any_value(question) as question,
        any_value(slug) as slug,
        any_value(active) as active,
        any_value(closed) as closed,
        any_value(archived) as archived,
        any_value(end_date) as end_date
    from numbered
    group by venue, market_id, version_no
),

watermark as (
    select max(batch_loaded_at) as built_through from sem
)

select
    v.venue,
    v.market_id,
    v.version_no,
    v.valid_from,
    lead(v.valid_from) over (partition by v.venue, v.market_id order by v.version_no) as valid_to,
    lead(v.valid_from) over (partition by v.venue, v.market_id order by v.version_no) is null as is_current,
    v.last_observed_at,
    v.observation_count,
    v.semantic_hash,
    v.question,
    v.slug,
    v.active,
    v.closed,
    v.archived,
    v.end_date,
    w.built_through
from versions v
cross join watermark w
