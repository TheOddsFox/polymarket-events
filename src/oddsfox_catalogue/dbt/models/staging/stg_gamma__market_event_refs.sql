select observation_id, venue, market_id, item.event_id, item.observation_id as membership_observation_id,
    item.source_kind as membership_source_kind, cast(item.received_at as timestamptz) as membership_received_at,
    cast(null as varchar) as event_slug, cast(null as varchar) as event_ticker
from (
    select observation_id, venue, market_id,
        unnest(json_transform(event_refs_json, '[{"event_id":"VARCHAR","observation_id":"VARCHAR","source_kind":"VARCHAR","received_at":"VARCHAR"}]')) as item
    from {{ ref('stg_gamma__market_observations') }}
)
