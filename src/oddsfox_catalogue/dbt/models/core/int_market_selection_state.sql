{{ config(materialized='incremental', incremental_strategy='delete+insert',
    unique_key=['venue', 'market_id'],
    pre_hook="{{ registered_batches_begin() }}",
    post_hook="{{ registered_batches_finish() }}") }}

with affected as (
    select distinct s.venue, s.market_id
    from {{ ref('stg_gamma__market_observations') }} s
    where {{ registered_batches_predicate('s.batch_id') }}
), ranked as (
    select s.venue, s.market_id, s.observation_id,
        count(*) over(partition by s.venue, s.market_id) as observation_count,
        min(s.observed_at) over(partition by s.venue, s.market_id) as first_observed_at,
        row_number() over(partition by s.venue, s.market_id
            order by (s.source_kind = 'market_direct') desc, s.observed_at desc,
                s.observation_id desc) as recency_rank
    from {{ ref('stg_gamma__market_observations') }} s
    join affected a using (venue, market_id)
)
select venue, market_id, observation_id, observation_count, first_observed_at
from ranked
where recency_rank = 1
