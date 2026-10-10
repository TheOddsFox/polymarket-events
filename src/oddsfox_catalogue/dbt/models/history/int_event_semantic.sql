{{ config(materialized='view') }}
select * from {{ ref('stg_gamma__event_observations') }}
