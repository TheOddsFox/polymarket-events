select observation_id from {{ ref('event_history') }} group by observation_id having count(*) <> 1
