{{ config(materialized='incremental', incremental_strategy='append',
    pre_hook='{{ registered_batches_begin() }}',
    post_hook='{{ registered_batches_finish() }}') }}
select observation_id, venue, event_id, observed_at, batch_loaded_at, volume, liquidity, open_interest
from {{ ref('stg_gamma__event_observations') }}

where {{ registered_batches_predicate('batch_id') }}
