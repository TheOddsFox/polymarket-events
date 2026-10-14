{{ config(materialized='view') }}

-- Preserve the original ranked view's nullable schema after replacing its window query.
select case when true then columns(s.*) else null end, r.observation_count, r.first_observed_at
from {{ ref('stg_gamma__market_observations') }} s
join {{ ref('int_market_selection_state') }} r using(observation_id)
