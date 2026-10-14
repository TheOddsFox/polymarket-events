{{ config(materialized='table') }}
select venue, asset_kind, asset_id, count(distinct market_id) as owner_count
from {{ ref('int_market_asset_nominations') }}
group by venue, asset_kind, asset_id
having count(distinct market_id) > 1
