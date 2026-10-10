{{ config(materialized='table') }}
-- Each observation is evidence; receipt order does not assert historical validity.
select * from {{ ref('int_market_semantic') }}
