-- Decodes the JSON-encoded outcome arrays of each current market and checks that they
-- align. Outcomes, prices, and token or position IDs are parallel arrays, so their lengths
-- must match. A market that fails the check is quarantined, not guessed at.
{{ config(materialized='view') }}

with decoded as (
    select
        venue,
        market_id,
        observation_id,
        projection_source_version,
        {{ decode_json_string_array('outcomes_raw') }} as outcomes,
        list_transform(
            {{ decode_json_string_array('outcome_prices_raw') }},
            x -> try_cast(x as double)
        ) as prices,
        {{ decode_json_string_array('clob_token_ids_raw') }} as clob_token_ids,
        {{ decode_json_string_array('position_ids_raw') }} as position_ids
    from {{ ref('markets_current') }}
),

checked as (
    select
        *,
        len(outcomes) as outcome_count,
        len(prices) as price_count,
        len(clob_token_ids) as clob_token_count,
        len(position_ids) as position_count,
        coalesce(
            outcomes is not null
            and prices is not null
            and len(outcomes) = len(prices)
            and (clob_token_ids is null or len(clob_token_ids) = len(outcomes))
            and (position_ids is null or len(position_ids) = len(outcomes))
            and len(outcomes) > 0,
            false
        ) as alignment_ok
    from decoded
)

select * from checked
