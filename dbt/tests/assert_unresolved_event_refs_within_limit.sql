-- Event references nested under markets must resolve to a captured current event. A reference
-- that does not resolve means the by-ID lookup failed or the event is missing, so the share
-- of unresolved references is capped by [quality] unresolved_reference_max_ratio. Returns a
-- row (fails the build) only when the unresolved count is above the cap. Compared by
-- multiplication so an empty bridge passes and a share exactly at the cap passes.
with refs as (
    select venue, market_id, event_id
    from {{ ref('market_event_bridge') }}
),

unresolved as (
    select r.venue, r.market_id, r.event_id
    from refs r
    left join {{ ref('events_current') }} e using (venue, event_id)
    where e.event_id is null
),

totals as (
    select
        (select count(*) from refs) as total_refs,
        (select count(*) from unresolved) as unresolved_refs
)

select
    total_refs,
    unresolved_refs,
    {{ var('max_unresolved_reference_ratio') }} as max_ratio
from totals
where unresolved_refs > {{ var('max_unresolved_reference_ratio') }} * total_refs
