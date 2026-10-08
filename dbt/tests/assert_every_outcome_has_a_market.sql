-- Every outcome row must point at a current market.
select o.*
from {{ ref('outcomes_current') }} o
left join {{ ref('markets_current') }} m using (venue, market_id)
where m.market_id is null
