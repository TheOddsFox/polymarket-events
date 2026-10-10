{{ config(materialized='table') }}
select observation_id, venue, event_id, observed_at, batch_loaded_at, volume, liquidity, open_interest
from {{ ref('stg_gamma__event_observations') }}
