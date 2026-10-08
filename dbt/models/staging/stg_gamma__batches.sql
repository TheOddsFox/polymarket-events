-- Only batches whose pages are all loaded are visible to dbt. The registry is written
-- last by the loader, so a half-loaded batch can never leak into the warehouse.
select
    batch_id,
    mode,
    observation_date,
    cast(loaded_at as timestamptz) as batch_loaded_at,
    page_count,
    event_observation_count,
    market_observation_count,
    quarantine_count
from {{ source('bronze', 'batch_registry') }}
where status = 'loaded'
