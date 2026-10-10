{{ config(materialized='table') }}
select m.* exclude(usable, identity_error, observed_at, batch_loaded_at),
    o.usable, o.identity_error, m.observed_at as last_observed_at, m.batch_loaded_at as built_through
from {{ ref('int_market_selected') }} m
join {{ ref('int_market_outcomes') }} o using(venue, market_id, observation_id)
