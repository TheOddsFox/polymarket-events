{{ config(materialized='table') }}
select s.* exclude(observed_at, batch_loaded_at),
    selected.observation_count, selected.first_observed_at,
    s.observed_at as last_observed_at, s.batch_loaded_at as built_through
from {{ ref('stg_gamma__event_observations') }} s
join {{ ref('int_event_selection_state') }} selected
    on s.venue = selected.venue and s.event_id = selected.event_id
    and s.observation_id = selected.observation_id
