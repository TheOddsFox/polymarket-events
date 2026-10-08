-- Meaning of a market. Prices, liquidity, and volume are excluded, as they are volatile.
-- Outcome labels are included, because renaming an outcome changes what the market means.
{{ config(materialized='view') }}

select
    observation_id,
    venue,
    market_id,
    observed_at,
    batch_loaded_at,
    question,
    slug,
    condition_id,
    active,
    closed,
    archived,
    end_date,
    outcomes_raw,
    sha256(concat_ws(
        '|',
        '{{ var("projection_version") }}',
        coalesce(question, ''),
        coalesce(slug, ''),
        coalesce(condition_id, ''),
        cast(active as varchar),
        cast(closed as varchar),
        cast(archived as varchar),
        coalesce(cast(end_date as varchar), ''),
        coalesce(outcomes_raw, '')
    )) as semantic_hash
from {{ ref('stg_gamma__market_observations') }}
