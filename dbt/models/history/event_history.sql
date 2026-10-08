-- SCD2 versions per event. Incremental on the same watermark as events_current: affected
-- entities are fully recomputed (every version), and delete+insert on (venue, event_id)
-- replaces their rows. Unaffected entities keep their rows untouched.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['venue', 'event_id'],
    on_schema_change='fail'
) }}

with sem as (
    select * from {{ ref('int_event_semantic') }}
),

{{ scoped_rows('sem', 'venue, event_id') }}

starts as (
    select
        *,
        case
            when semantic_hash is distinct from lag(semantic_hash) over (
                partition by venue, event_id order by observed_at, observation_id
            ) then 1
            else 0
        end as starts_version
    from scoped
),

numbered as (
    select
        *,
        sum(starts_version) over (
            partition by venue, event_id
            order by observed_at, observation_id
            rows between unbounded preceding and current row
        ) as version_no
    from starts
),

versions as (
    select
        venue,
        event_id,
        version_no,
        min(observed_at) as valid_from,
        max(observed_at) as last_observed_at,
        count(*) as observation_count,
        any_value(semantic_hash) as semantic_hash,
        any_value(title) as title,
        any_value(slug) as slug,
        any_value(active) as active,
        any_value(closed) as closed,
        any_value(archived) as archived,
        any_value(end_date) as end_date
    from numbered
    group by venue, event_id, version_no
),

{{ batch_watermark('sem') }}

select
    v.venue,
    v.event_id,
    v.version_no,
    v.valid_from,
    lead(v.valid_from) over (partition by v.venue, v.event_id order by v.version_no) as valid_to,
    lead(v.valid_from) over (partition by v.venue, v.event_id order by v.version_no) is null as is_current,
    v.last_observed_at,
    v.observation_count,
    v.semantic_hash,
    v.title,
    v.slug,
    v.active,
    v.closed,
    v.archived,
    v.end_date,
    w.built_through
from versions v
cross join watermark w
