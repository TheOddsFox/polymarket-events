{{ config(materialized='view') }}
with conflicted_markets as (
    select distinct n.venue, n.market_id
    from {{ ref('int_market_asset_nominations') }} n
    join {{ ref('int_market_asset_conflicts') }} c
        on n.venue = c.venue and n.asset_kind = c.asset_kind and n.asset_id = c.asset_id
)
select m.venue, m.market_id, m.observation_id, m.outcomes_json,
    m.usable and c.market_id is null as usable,
    case when c.market_id is not null then 'native identity has conflicting owners' else m.identity_error end as identity_error
from {{ ref('int_market_selected') }} m
left join conflicted_markets c using(venue, market_id)
