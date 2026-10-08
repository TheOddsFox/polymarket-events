-- Latest known state of every event. Incremental: only entities that appear in batches
-- registered since the last build are recomputed. The delete+insert strategy replaces each
-- affected entity's row, so no stale state survives.
--
-- built_through records the newest batch folded in. The next build uses the maximum of
-- this column as its watermark, so a build that writes no rows does not advance it.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['venue', 'event_id'],
    on_schema_change='fail'
) }}

with obs as (
    select * from {{ ref('stg_gamma__event_observations') }}
),

{% if is_incremental() %}
scope as (
    select distinct venue, event_id
    from obs
    where batch_loaded_at >= (
        select coalesce(max(built_through), timestamp '1970-01-01 00:00:00+00') from {{ this }}
    )
),

scoped as (
    select o.*
    from obs o
    join scope s using (venue, event_id)
),
{% else %}
scoped as (
    select * from obs
),
{% endif %}

ranked as (
    select
        *,
        row_number() over (
            partition by venue, event_id
            order by observed_at desc, observation_id desc
        ) as recency_rank,
        count(*) over (partition by venue, event_id) as observation_count,
        min(observed_at) over (partition by venue, event_id) as first_observed_at
    from scoped
),

watermark as (
    select max(batch_loaded_at) as built_through from obs
)

select
    r.venue,
    r.event_id,
    r.observation_id,
    r.batch_id,
    r.title,
    r.slug,
    r.ticker,
    r.active,
    r.closed,
    r.archived,
    r.neg_risk,
    r.start_date,
    r.end_date,
    r.volume,
    r.liquidity,
    r.open_interest,
    r.source_updated_at,
    r.payload_hash,
    r.tags_present,
    r.series_present,
    r.first_observed_at,
    r.observed_at as last_observed_at,
    r.observation_count,
    w.built_through
from ranked r
cross join watermark w
where r.recency_rank = 1
