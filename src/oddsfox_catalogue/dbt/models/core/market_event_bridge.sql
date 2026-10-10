{{ config(materialized='table') }}
with explicit as (
    select r.venue, r.market_id, r.event_id, r.event_slug, r.event_ticker,
        r.membership_observation_id, r.membership_source_kind, r.membership_received_at,
        false as inferred
    from {{ ref('stg_gamma__market_event_refs') }} r
    join {{ ref('markets_current') }} m using(observation_id, venue, market_id)
    where m.membership_mode = 'explicit'
),
inferred as (
    select o.venue, o.market_id, o.enclosing_event_id as event_id,
        cast(null as varchar) as event_slug, cast(null as varchar) as event_ticker,
        o.observation_id as membership_observation_id, o.source_kind as membership_source_kind,
        o.observed_at as membership_received_at, true as inferred
    from {{ ref('stg_gamma__market_observations') }} o
    join {{ ref('markets_current') }} m using(venue, market_id)
    where m.membership_mode = 'missing' and o.enclosing_event_id is not null
    qualify row_number() over(partition by o.venue, o.market_id, o.enclosing_event_id
        order by (o.source_kind = 'market_direct') desc, o.observed_at desc, o.observation_id desc) = 1
)
select * from explicit
union all
select * from inferred
