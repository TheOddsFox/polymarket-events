-- Latest known state of every market. Same incremental contract as events_current.
-- Array-valued fields stay raw here; outcomes_current decodes and checks alignment.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['venue', 'market_id'],
    on_schema_change='fail'
) }}

with obs as (
    select * from {{ ref('stg_gamma__market_observations') }}
),

{{ scoped_rows('obs', 'venue, market_id') }}

ranked as (
    select
        *,
        row_number() over (
            partition by venue, market_id
            order by observed_at desc, observation_id desc
        ) as recency_rank,
        count(*) over (partition by venue, market_id) as observation_count,
        min(observed_at) over (partition by venue, market_id) as first_observed_at
    from scoped
),

{{ batch_watermark('obs') }}

select
    r.venue,
    r.market_id,
    r.observation_id,
    r.batch_id,
    r.source_kind,
    r.json_pointer,
    r.projection_source_version,
    r.question,
    r.slug,
    r.condition_id,
    r.active,
    r.closed,
    r.archived,
    r.end_date,
    r.volume,
    r.liquidity,
    r.outcomes_raw,
    r.outcome_prices_raw,
    r.clob_token_ids_raw,
    r.position_ids_raw,
    r.source_updated_at,
    r.payload_hash,
    r.event_refs_present,
    r.tags_present,
    r.first_observed_at,
    r.observed_at as last_observed_at,
    r.observation_count,
    w.built_through
from ranked r
cross join watermark w
where r.recency_rank = 1
