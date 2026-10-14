{{ config(materialized='incremental', incremental_strategy='append',
    pre_hook='{{ registered_batches_begin() }}',
    post_hook='{{ registered_batches_finish() }}') }}
-- Each observation is evidence; receipt order does not assert historical validity.
select * from {{ ref('int_event_semantic') }}

where {{ registered_batches_predicate('batch_id') }}
