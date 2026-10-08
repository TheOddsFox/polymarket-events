-- Event identity stubs nested under a market. These are references, not event observations;
-- the event itself is captured and loaded through its own pages.
select
    observation_id,
    venue,
    market_id,
    ref.id as event_id,
    ref.slug as event_slug,
    ref.ticker as event_ticker
from (
    select
        observation_id,
        venue,
        market_id,
        unnest(json_transform(event_refs_json, '[{"id":"VARCHAR","slug":"VARCHAR","ticker":"VARCHAR"}]')) as ref
    from {{ ref('stg_gamma__market_observations') }}
    where event_refs_present
)
