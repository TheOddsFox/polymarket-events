{{ config(materialized='view') }}
-- Rank envelope fields before fetching the winning row's payload.
with ranked as (
    select observation_id, row_number() over(partition by venue, market_id
            order by (source_kind = 'market_direct') desc, observed_at desc, observation_id desc) as recency_rank,
        count(*) over(partition by venue, market_id) as observation_count,
        min(observed_at) over(partition by venue, market_id) as first_observed_at
    from {{ ref('stg_gamma__market_observations') }}
)
select s.*, r.observation_count, r.first_observed_at
from {{ ref('stg_gamma__market_observations') }} s
join ranked r using(observation_id)
where r.recency_rank = 1
