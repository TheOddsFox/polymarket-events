{{ config(materialized='view') }}
with nominated as (
    select venue, market_id, unnest(json_transform(outcomes_json,
        '[{"asset_kind":"VARCHAR","asset_id":"VARCHAR","condition_id":"VARCHAR"}]')) as identity
    from {{ ref('int_market_selected') }} where usable
),
conflicts as (
    select venue, identity.asset_kind as asset_kind, identity.asset_id as asset_id
    from nominated
    group by venue, identity.asset_kind, identity.asset_id
    having count(distinct market_id) > 1
),
conflicted_markets as (
    select distinct n.venue, n.market_id from nominated n join conflicts c
        on n.venue = c.venue and n.identity.asset_kind = c.asset_kind and n.identity.asset_id = c.asset_id
)
select m.venue, m.market_id, m.observation_id, m.outcomes_json,
    m.usable and c.market_id is null as usable,
    case when c.market_id is not null then 'native identity has conflicting owners' else m.identity_error end as identity_error
from {{ ref('int_market_selected') }} m
left join conflicted_markets c using(venue, market_id)
