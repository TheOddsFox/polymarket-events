-- Tags of the latest market observation. Rebuilt in full from the current state.
{{ config(materialized='table') }}

select t.venue, t.market_id, t.tag_id, t.tag_label, t.tag_slug
from {{ ref('stg_gamma__market_tags') }} t
join {{ ref('markets_current') }} m using (observation_id, venue, market_id)
