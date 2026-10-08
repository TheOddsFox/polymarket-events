-- Tags of the latest event observation. Rebuilt in full from the current state.
{{ config(materialized='table') }}

select t.venue, t.event_id, t.tag_id, t.tag_label, t.tag_slug
from {{ ref('stg_gamma__event_tags') }} t
join {{ ref('events_current') }} e using (observation_id, venue, event_id)
