{{ config(materialized='table') }}
select venue, market_id, identity.condition_id, identity.outcome_index, identity.outcome_label,
    identity.asset_kind, identity.asset_id, identity.clob_token_id, identity.position_id,
    identity.chain_index_set, observation_id
from (
    select venue, market_id, observation_id,
        unnest(json_transform(outcomes_json,
            '[{"condition_id":"VARCHAR","outcome_index":"INTEGER","outcome_label":"VARCHAR","asset_kind":"VARCHAR","asset_id":"VARCHAR","clob_token_id":"VARCHAR","position_id":"VARCHAR","chain_index_set":"INTEGER"}]')) as identity
    from {{ ref('int_market_outcomes') }} where usable
)
