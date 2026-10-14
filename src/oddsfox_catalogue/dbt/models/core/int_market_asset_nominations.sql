{{ config(materialized='table') }}
with nominated as (
    select venue, market_id, unnest(json_transform(outcomes_json,
        '[{"asset_kind":"VARCHAR","asset_id":"VARCHAR"}]')) as identity
    from {{ ref('int_market_selected') }} where usable
)
select venue, market_id, identity.asset_kind as asset_kind, identity.asset_id as asset_id
from nominated
