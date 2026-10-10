{{ config(materialized='view') }}
select * from {{ ref('stg_gamma__market_observations') }}
