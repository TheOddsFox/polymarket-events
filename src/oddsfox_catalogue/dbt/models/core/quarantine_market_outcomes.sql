{{ config(materialized='view') }}
select venue, market_id, observation_id, identity_error
from {{ ref('int_market_outcomes') }} where not usable
