-- Critical: a market whose outcome arrays do not line up means the upstream format changed.
-- Publication must not proceed, so this test fails the build on any row.
select *
from {{ ref('quarantine_market_outcomes') }}
