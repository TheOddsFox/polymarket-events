-- Volatile market measurements. Append-only, one row per observation.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='observation_id'
) }}

select
    observation_id,
    venue,
    market_id,
    observed_at,
    batch_loaded_at,
    volume,
    liquidity
from {{ ref('stg_gamma__market_observations') }}
{% if is_incremental() %}
where observation_id not in (select observation_id from {{ this }})
{% endif %}
