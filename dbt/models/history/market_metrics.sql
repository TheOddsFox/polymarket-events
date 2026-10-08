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
-- Only batches loaded since the last build. The ">=" re-reads rows at the watermark itself;
-- delete+insert on observation_id makes that re-read idempotent.
where batch_loaded_at >= (
    select coalesce(max(batch_loaded_at), timestamp '1970-01-01 00:00:00+00') from {{ this }}
)
{% endif %}
