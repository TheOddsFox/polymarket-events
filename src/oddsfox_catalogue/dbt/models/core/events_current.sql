{{ config(materialized='table') }}
select * exclude(recency_rank, observed_at, batch_loaded_at), observed_at as last_observed_at,
    batch_loaded_at as built_through
from (
    select *, row_number() over(partition by venue, event_id order by observed_at desc, observation_id desc) as recency_rank,
        count(*) over(partition by venue, event_id) as observation_count,
        min(observed_at) over(partition by venue, event_id) as first_observed_at
    from {{ ref('stg_gamma__event_observations') }}
)
where recency_rank = 1
