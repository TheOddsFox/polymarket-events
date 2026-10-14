{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['venue', 'event_id'],
    pre_hook="{{ registered_batches_begin('1') }}",
    post_hook="{{ registered_batches_finish('1') }}"
) }}

with affected as (
    select distinct venue, event_id
    from {{ ref('stg_gamma__event_observations') }}
    where {{ registered_batches_predicate('batch_id') }}
), ranked as (
    select o.venue, o.event_id, o.observation_id,
        row_number() over(partition by o.venue, o.event_id
            order by o.observed_at desc, o.observation_id desc) as recency_rank,
        count(*) over(partition by o.venue, o.event_id) as observation_count,
        min(o.observed_at) over(partition by o.venue, o.event_id) as first_observed_at
    from {{ ref('stg_gamma__event_observations') }} o
    join affected using (venue, event_id)
)
select venue, event_id, observation_id, observation_count, first_observed_at
from ranked
where recency_rank = 1
