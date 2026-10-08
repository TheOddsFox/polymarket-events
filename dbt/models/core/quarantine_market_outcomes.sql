-- Markets whose outcome arrays do not align. They are excluded from outcomes_current and
-- counted by a test, so an upstream format change shows up as a failed build, not bad rows.
{{ config(materialized='view') }}

select
    venue,
    market_id,
    observation_id,
    outcome_count,
    price_count,
    clob_token_count,
    position_count,
    'outcome arrays are not aligned' as reason
from {{ ref('int_market_outcomes') }}
where not alignment_ok
