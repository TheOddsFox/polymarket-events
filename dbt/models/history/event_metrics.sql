-- Volatile event measurements. Append-only: each observation contributes one row, once.
-- Restating an observation is impossible by construction, because the key is observation_id.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='observation_id'
) }}

select
    observation_id,
    venue,
    event_id,
    observed_at,
    batch_loaded_at,
    volume,
    liquidity,
    open_interest
from {{ ref('stg_gamma__event_observations') }}
{% if is_incremental() %}
-- Only batches loaded since the last build. The ">=" re-reads rows at the watermark itself;
-- delete+insert on observation_id makes that re-read idempotent.
where batch_loaded_at >= (
    select coalesce(max(batch_loaded_at), timestamp '1970-01-01 00:00:00+00') from {{ this }}
)
{% endif %}
