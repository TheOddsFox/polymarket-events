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
where observation_id not in (select observation_id from {{ this }})
{% endif %}
