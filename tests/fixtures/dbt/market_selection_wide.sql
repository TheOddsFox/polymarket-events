{{ config(materialized='view') }}
select * exclude(recency_rank)
from (
    select *, row_number() over(partition by venue, market_id
            order by (source_kind = 'market_direct') desc, observed_at desc, observation_id desc) as recency_rank,
        count(*) over(partition by venue, market_id) as observation_count,
        min(observed_at) over(partition by venue, market_id) as first_observed_at
    from {{ ref('stg_gamma__market_observations') }}
)
where recency_rank = 1
