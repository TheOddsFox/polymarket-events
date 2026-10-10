-- Series of the latest event observation. Rebuilt in full from the current state.
{{ config(materialized='table') }}

select s.venue, s.event_id, s.series_id, s.series_slug, s.series_title
from {{ ref('stg_gamma__event_series') }} s
join {{ ref('events_current') }} e using (observation_id, venue, event_id)
