-- Usable nominations must be unique within their typed native identity.
select venue, asset_kind, asset_id from {{ ref('outcomes_current') }}
group by venue, asset_kind, asset_id having count(*) <> 1
