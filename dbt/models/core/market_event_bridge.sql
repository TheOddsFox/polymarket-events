-- Which events a current market is nested under. A market can belong to more than one
-- event stub, so this is a bridge table. Rebuilt in full from the current state.
{{ config(materialized='table') }}

select r.venue, r.market_id, r.event_id, r.event_slug, r.event_ticker
from {{ ref('stg_gamma__market_event_refs') }} r
join {{ ref('markets_current') }} m using (observation_id, venue, market_id)
