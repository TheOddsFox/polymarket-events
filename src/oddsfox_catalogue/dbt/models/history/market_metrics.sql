{{ config(materialized='table') }}
select observation_id, venue, market_id, observed_at, batch_loaded_at, volume, liquidity
from {{ ref('stg_gamma__market_observations') }}
