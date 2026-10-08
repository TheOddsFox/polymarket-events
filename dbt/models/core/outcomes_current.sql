-- One row per outcome of every aligned current market. Rebuilt in full because it is
-- cheap, and a full rebuild keeps outcome indexes in step with the parent market.
{{ config(materialized='table') }}

with expanded as (
    select
        c.*,
        unnest(generate_series(1, len(c.outcomes))) as outcome_index
    from {{ ref('int_market_outcomes') }} c
    where c.alignment_ok
)

select
    venue,
    market_id,
    outcome_index,
    outcomes[outcome_index] as outcome_label,
    prices[outcome_index] as outcome_price,
    clob_token_ids[outcome_index] as clob_token_id,
    position_ids[outcome_index] as position_id,
    projection_source_version,
    observation_id
from expanded
