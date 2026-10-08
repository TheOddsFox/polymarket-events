-- Typed view over event observations from registered batches. Payload fields are
-- extracted with try_cast, so a malformed value becomes NULL instead of failing the build.
select
    observation_id,
    venue,
    entity_id as event_id,
    batch_id,
    page_id,
    endpoint,
    observed_at,
    b.batch_loaded_at,
    source_updated_at,
    payload_hash,
    json_extract_string(payload, '$.title') as title,
    json_extract_string(payload, '$.slug') as slug,
    json_extract_string(payload, '$.ticker') as ticker,
    coalesce(try_cast(json_extract_string(payload, '$.active') as boolean), false) as active,
    coalesce(try_cast(json_extract_string(payload, '$.closed') as boolean), false) as closed,
    coalesce(try_cast(json_extract_string(payload, '$.archived') as boolean), false) as archived,
    coalesce(try_cast(json_extract_string(payload, '$.negRisk') as boolean), false) as neg_risk,
    try_cast(json_extract_string(payload, '$.startDate') as timestamptz) as start_date,
    try_cast(json_extract_string(payload, '$.endDate') as timestamptz) as end_date,
    try_cast(json_extract_string(payload, '$.volume') as double) as volume,
    try_cast(json_extract_string(payload, '$.liquidity') as double) as liquidity,
    try_cast(json_extract_string(payload, '$.openInterest') as double) as open_interest,
    {{ json_array_present('payload', '$.tags') }} as tags_present,
    {{ json_array_present('payload', '$.series') }} as series_present,
    json_extract(payload, '$.tags') as tags_json,
    json_extract(payload, '$.series') as series_json
from {{ source('bronze', 'event_observations') }} o
join {{ ref('stg_gamma__batches') }} b using (batch_id)
