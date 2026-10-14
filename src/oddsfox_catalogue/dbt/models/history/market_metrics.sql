{{ config(materialized='incremental', incremental_strategy='append',
    pre_hook='{{ registered_batches_begin() }}',
    post_hook='{{ registered_batches_finish() }}') }}
select observation_id, venue, market_id, observed_at, batch_loaded_at, volume, liquidity
from {{ ref('stg_gamma__market_observations') }}

where {{ registered_batches_predicate('batch_id') }}
