-- Meaning of an event. Two observations with the same semantic hash describe the same event.
-- Volume, liquidity, and open interest are excluded: they change constantly and belong in
-- event_metrics, not in history. The projection version is hashed in so a change to what
-- counts as "meaning" produces new hashes and a deliberate history rebuild.
{{ config(materialized='view') }}

select
    observation_id,
    venue,
    event_id,
    observed_at,
    batch_loaded_at,
    title,
    slug,
    ticker,
    active,
    closed,
    archived,
    neg_risk,
    end_date,
    sha256(concat_ws(
        '|',
        '{{ var("projection_version") }}',
        coalesce(title, ''),
        coalesce(slug, ''),
        coalesce(ticker, ''),
        cast(active as varchar),
        cast(closed as varchar),
        cast(archived as varchar),
        cast(neg_risk as varchar),
        coalesce(cast(end_date as varchar), '')
    )) as semantic_hash
from {{ ref('stg_gamma__event_observations') }}
