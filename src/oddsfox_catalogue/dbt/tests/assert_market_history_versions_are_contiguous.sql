select observation_id from {{ ref('market_history') }} group by observation_id having count(*) <> 1
